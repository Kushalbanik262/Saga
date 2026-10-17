import { randomUUID } from "node:crypto";
import {
    createConnectionAndReturnChannel,
    errorDetails,
    log,
    publishToTopic,
    subscribeToTopic
} from "./commns/connections.js";
import {
    action,
    INV_ORCH_REQUEST_QUEUE,
    INV_ORCH_REQUEST_ROUTING_KEY
} from "./commns/orchastratorclient.js";

const SERVICE = "orchestrator";
let channel;

async function sendInventoryAction(envelope, requestedAction) {
    const message = envelope.message;
    const response = {
        action: requestedAction,
        id: message.itemId,
        quantity: message.itemQuantity
    };
    await publishToTopic(
        channel,
        `inventory.response.${requestedAction.toLowerCase()}`,
        {
            message: response,
            correlationId: envelope.correlationId,
            messageId: randomUUID(),
            time: new Date().toISOString()
        },
        SERVICE
    );
    log(SERVICE, "INFO", "inventory.action.published", {
        correlationId: envelope.correlationId,
        action: requestedAction,
        itemId: message.itemId,
        quantity: message.itemQuantity
    });
}

async function handleInventoryCommand(envelope) {
    const message = envelope.message;
    if (!message?.type) throw new Error("Inventory command is missing type");

    log(SERVICE, "INFO", "inventory.command.processing", {
        correlationId: envelope.correlationId,
        type: message.type,
        itemId: message.itemId,
        quantity: message.itemQuantity
    });

    if (message.type === action.ADD) await sendInventoryAction(envelope, "PROCEED");
    else if (message.type === action.ROLLBACK) await sendInventoryAction(envelope, "ROLLBACK");
    else throw new Error(`Unsupported inventory command: ${message.type}`);
}

async function listenToInventory() {
    channel = await createConnectionAndReturnChannel(SERVICE);
    await subscribeToTopic(channel, {
        service: SERVICE,
        queue: INV_ORCH_REQUEST_QUEUE,
        routingKeys: [INV_ORCH_REQUEST_ROUTING_KEY],
        handler: handleInventoryCommand
    });
}

listenToInventory().catch((error) => {
    log(SERVICE, "ERROR", "topic.listener.failed", { error: errorDetails(error) });
    process.exitCode = 1;
});
