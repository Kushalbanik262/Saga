import { randomUUID } from "node:crypto";
import { publishToTopic } from "./connections.js";

export const INV_ORCH_REQUEST_QUEUE = "orchestrator.inventory.commands";
export const INV_ORCH_RESPONSE_QUEUE = "inventory.orchestrator.responses";
export const INV_ORCH_REQUEST_ROUTING_KEY = "inventory.command.#";
export const INV_ORCH_RESPONSE_ROUTING_KEY = "inventory.response.#";


// Payment Related Queues
export const PAY_ORCH_REQUEST_QUEUE = "orchestrator.payment.commands";
export const PAY_ORCH_RESPONSE_QUEUE = "payment.orchestrator.responses"
export const PAY_ORCH_REQUEST_ROUTING_KEY = "payment.command.#";
export const PAY_ORCH_RESPONSE_ROUTING_KEY = "payment.response.#";


// Inventory orchestrator routing


export const action = Object.freeze({ ADD: "add", ROLLBACK: "rollback" });

export async function postToOrchastrator(message, connectionChannel,service="unknown") {
    if (connectionChannel == null) throw new Error("Invalid RabbitMQ channel");

    const envelope = {
        message,
        correlationId: message.correlationId ?? randomUUID(),
        messageId: randomUUID(),
        time: new Date().toISOString()
    };

    await publishToTopic(
        connectionChannel,
        `${service}.command.${message.type}`,
        envelope,
        service
    );
}
