import express from "express";
import { randomUUID } from "node:crypto";
import {
    action,
    INV_ORCH_RESPONSE_QUEUE,
    INV_ORCH_RESPONSE_ROUTING_KEY,
    postToOrchastrator
} from "./commns/orchastratorclient.js";
import {
    createConnectionAndReturnChannel,
    errorDetails,
    log,
    subscribeToTopic
} from "./commns/connections.js";
import { createCompensationLedger } from "./domain/compensationLedger.js";

const SERVICE = "inventory";
const PORT = 8081;
const app = express();
const inventoryLedger = createCompensationLedger([
    ["rice", 2], ["shampoo", 20], ["headphone", 3]
]);
const map = inventoryLedger.balances;
let channel;

app.use(express.json());

app.post("/buy/items", async (req, res) => {
    const correlationId = req.get("x-correlation-id") ?? randomUUID();
    const request = { ...req.body, correlationId };

    if (request.userId == null || request.userId === "") {
        return res.status(400).json({ message: "userId is required", correlationId });
    }

    log(SERVICE, "INFO", "inventory.purchase.requested", {
        correlationId,
        itemId: request.id,
        quantity: request.quantity
    });

    try {
        await commandToOrchestrator(request, action.ADD);
        return res.status(202).json({
            message: "Inventory operation accepted",
            correlationId,
            itemId: request.id,
            quantity: request.quantity,
            userId: request.userId
        });
    } catch (error) {
        log(SERVICE, "ERROR", "inventory.purchase.request.failed", {
            correlationId,
            itemId: request.id,
            quantity: request.quantity,
            error: errorDetails(error)
        });
        return res.status(500).json({ message: "Unable to submit inventory operation", correlationId });
    }
});

app.post("/rollback", async (req, res) => {
    const correlationId = req.get("x-correlation-id") ?? randomUUID();
    const { id, quantity } = req.body;
    try {
        if (!rollbackInventory(correlationId, id, quantity)) {
            throw new Error(`No inventory transaction found for rollback ${correlationId}`);
        }
        log(SERVICE, "INFO", "inventory.rollback.completed", {
            correlationId, itemId: id, quantity, remainingQuantity: map.get(id)
        });
        return res.status(200).json({ itemId: id, quantity: map.get(id), correlationId });
    } catch (error) {
        log(SERVICE, "ERROR", "inventory.rollback.failed", {
            correlationId, itemId: id, quantity, error: errorDetails(error)
        });
        return res.status(400).json({ message: "Inventory rollback failed", correlationId });
    }
});

function increaseInventory(itemId, quantity) {
    const parsedQuantity = parseInt(quantity, 10);
    if (itemId == null || !map.has(itemId) || !Number.isFinite(parsedQuantity) || parsedQuantity < 0) {
        throw new Error(`Invalid rollback item or quantity: ${itemId}, ${quantity}`);
    }
    map.set(itemId, map.get(itemId) + parsedQuantity);
}

function reduceInventory(itemId, quantity) {
    const parsedQuantity = parseInt(quantity, 10);
    if (itemId == null || !map.has(itemId)) throw new Error(`Item ${itemId} not found`);
    if (!Number.isFinite(parsedQuantity) || parsedQuantity <= 0) throw new Error(`Invalid quantity: ${quantity}`);
    if (map.get(itemId) < parsedQuantity) throw new Error(`Insufficient inventory for item ${itemId}`);
    map.set(itemId, map.get(itemId) - parsedQuantity);
    return map.get(itemId);
}

async function commandToOrchestrator(request, commandType) {
    const command = {
        type: commandType,
        itemId: request.itemId ?? request.id,
        itemQuantity: request.quantity,
        correlationId: request.correlationId,
        userId: request.userId
    };
    log(SERVICE,"DEBUG","inventory.orchastrator.command",command);
    if (channel == null) channel = await createConnectionAndReturnChannel(SERVICE);
    await postToOrchastrator(command, channel,SERVICE);
}

async function executeAction(envelope) {
    const message = envelope.message ?? envelope;
    const { action: requestedAction, id, quantity } = message;
    log(SERVICE, "INFO", "inventory.action.started", {
        correlationId: envelope.correlationId, action: requestedAction, itemId: id, quantity
    });

    if (requestedAction === "PROCEED") {
        try {
            const remainingQuantity = reduceInventory(id, quantity);
            inventoryLedger.record(envelope.correlationId, id, parseInt(quantity, 10));
            log(SERVICE, "INFO", "inventory.reduction.completed", {
                correlationId: envelope.correlationId, itemId: id, quantity, remainingQuantity
            });
        } catch (error) {
            log(SERVICE, "ERROR", "inventory.reduction.failed", {
                correlationId: envelope.correlationId, itemId: id, quantity, error: errorDetails(error)
            });
            await commandToOrchestrator({ ...message, correlationId: envelope.correlationId }, action.ROLLBACK);
        }
    } else if (requestedAction === "ROLLBACK") {
        if (!rollbackInventory(envelope.correlationId, id, quantity)) {
            log(SERVICE, "WARN", "inventory.rollback.ignored", {
                correlationId: envelope.correlationId,
                itemId: id,
                quantity,
                reason: "No successful inventory transaction found"
            });
            return;
        }
        log(SERVICE, "INFO", "inventory.rollback.completed", {
            correlationId: envelope.correlationId, itemId: id, quantity, remainingQuantity: map.get(id)
        });
    } else {
        throw new Error(`Unsupported inventory action: ${requestedAction}`);
    }
}

function rollbackInventory(correlationId, itemId, quantity) {
    const parsedQuantity = parseInt(quantity, 10);
    return inventoryLedger.rollback(correlationId, itemId, parsedQuantity);
}

async function listenToOrchestrator() {
    if (channel == null) channel = await createConnectionAndReturnChannel(SERVICE);
    await subscribeToTopic(channel, {
        service: SERVICE,
        queue: INV_ORCH_RESPONSE_QUEUE,
        routingKeys: [INV_ORCH_RESPONSE_ROUTING_KEY],
        handler: executeAction
    });
}

app.listen(PORT, () => log(SERVICE, "INFO", "http.started", { port: PORT }));
listenToOrchestrator().catch((error) => {
    log(SERVICE, "ERROR", "topic.listener.failed", { error: errorDetails(error) });
    process.exitCode = 1;
});
