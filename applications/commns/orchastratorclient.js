import { randomUUID } from "node:crypto";
import { publishToTopic } from "./connections.js";

export const INV_ORCH_REQUEST_QUEUE = "orchestrator.inventory.commands";
export const INV_ORCH_RESPONSE_QUEUE = "inventory.orchestrator.responses";
export const INV_ORCH_REQUEST_ROUTING_KEY = "inventory.command.#";
export const INV_ORCH_RESPONSE_ROUTING_KEY = "inventory.response.#";

export const action = Object.freeze({ ADD: "add", ROLLBACK: "rollback" });

export async function postToOrchastrator(message, connectionChannel) {
    if (connectionChannel == null) throw new Error("Invalid RabbitMQ channel");

    const envelope = {
        message,
        correlationId: message.correlationId ?? randomUUID(),
        messageId: randomUUID(),
        time: new Date().toISOString()
    };

    await publishToTopic(
        connectionChannel,
        `inventory.command.${message.type}`,
        envelope,
        "inventory"
    );
}
