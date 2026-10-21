import assert from "node:assert/strict";
import amqp from "amqplib";
import { randomUUID } from "node:crypto";
import { PAY_ORCH_REQUEST_ROUTING_KEY } from "../../applications/commns/orchastratorclient.js";
import { TOPIC_EXCHANGE } from "../../applications/commns/connections.js";

const rabbitUrl = process.env.RABBITMQ_URL ??
    "amqp://admin:SuperSecretPassword123@localhost:5672/%2f";

export async function runRabbitmqRoutingTest() {
    let connection;
    try {
        connection = await amqp.connect(rabbitUrl);
    } catch {
        return { skipped: true };
    }

    const channel = await connection.createChannel();
    const queue = await channel.assertQueue("", { exclusive: true, autoDelete: true });
    await channel.assertExchange(TOPIC_EXCHANGE, "topic", { durable: true });
    await channel.bindQueue(queue.queue, TOPIC_EXCHANGE, PAY_ORCH_REQUEST_ROUTING_KEY);

    const correlationId = randomUUID();
    const envelope = {
        message: {
            type: "rollback",
            userId: "kushal",
            amount: 20,
            itemId: "rice",
            itemQuantity: 2
        },
        correlationId,
        messageId: randomUUID()
    };

    const received = new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("Timed out waiting for payment command")), 3000);
        channel.consume(queue.queue, (rawMessage) => {
            clearTimeout(timer);
            resolve(JSON.parse(rawMessage.content.toString()));
            channel.ack(rawMessage);
        }, { noAck: false });
    });

    channel.publish(
        TOPIC_EXCHANGE,
        "payment.command.rollback",
        Buffer.from(JSON.stringify(envelope)),
        { persistent: true, contentType: "application/json" }
    );

    const message = await received;
    assert.equal(message.correlationId, correlationId);
    assert.equal(message.message.type, "rollback");
    assert.equal(message.message.itemQuantity, 2);

    await channel.close();
    await connection.close();
    return { skipped: false };
}
