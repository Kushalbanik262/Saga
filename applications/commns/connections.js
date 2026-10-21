import amqp from "amqplib";

const RABBITMQ_URL = "amqp://admin:SuperSecretPassword123@localhost:5672/%2f";
export const TOPIC_EXCHANGE = "saga.topic";

function serializeError(error) {
    return { name: error?.name, message: error?.message, stack: error?.stack };
}

export function log(service, level, event, details = {}) {
    const fields = [];

    const appendFields = (prefix, value) => {
        if (value !== null && typeof value === "object") {
            for (const [key, nestedValue] of Object.entries(value)) {
                appendFields(`${prefix}.${key}`, nestedValue);
            }
            return;
        }

        const text = String(value ?? "").replace(/\r?\n/g, "\\n");
        const formattedText = text.includes(" ") ? `"${text.replace(/"/g, '\\"')}"` : text;
        fields.push(`${prefix}=${formattedText}`);
    };

    for (const [key, value] of Object.entries(details)) {
        appendFields(key, value);
    }

    const output = `[${new Date().toISOString()}] [${service}] [${level}] ${event}`
        + (fields.length > 0 ? ` | ${fields.join(" ")}` : "");
    if (level === "ERROR") console.error(output);
    else if (level === "WARN") console.warn(output);
    else console.log(output);
}

export function errorDetails(error) {
    return serializeError(error);
}

export async function createConnectionAndReturnChannel(service = "unknown") {
    log(service, "INFO", "rabbitmq.connecting", { exchange: TOPIC_EXCHANGE });
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();
    await channel.assertExchange(TOPIC_EXCHANGE, "topic", { durable: true });
    log(service, "INFO", "rabbitmq.connected", { exchange: TOPIC_EXCHANGE });
    return channel;
}

export async function subscribeToTopic(channel, { service, queue, routingKeys, handler }) {
    await channel.assertQueue(queue, { durable: true });
    for (const routingKey of routingKeys) {
        await channel.bindQueue(queue, TOPIC_EXCHANGE, routingKey);
    }

    log(service, "INFO", "topic.subscribed", { queue, routingKeys });
    await channel.consume(queue, async (rawMessage) => {
        if (rawMessage === null) {
            log(service, "WARN", "topic.consumer.cancelled", { queue });
            return;
        }

        let message;
        try {
            message = JSON.parse(rawMessage.content.toString());
            log(service, "INFO", "topic.message.received", {
                queue,
                routingKey: rawMessage.fields.routingKey,
                correlationId: message.correlationId,
                messageId: message.messageId
            });
            await handler(message, rawMessage.fields.routingKey);
            channel.ack(rawMessage);
            log(service, "INFO", "topic.message.acknowledged", {
                queue,
                routingKey: rawMessage.fields.routingKey,
                correlationId: message.correlationId,
                messageId: message.messageId
            });
        } catch (error) {
            log(service, "ERROR", "topic.message.failed", {
                queue,
                routingKey: rawMessage.fields.routingKey,
                correlationId: message?.correlationId,
                messageId: message?.messageId,
                error: serializeError(error)
            });
            channel.nack(rawMessage, false, false);
        }
    });
}

export async function publishToTopic(channel, routingKey, message, service) {
    const published = channel.publish(
        TOPIC_EXCHANGE,
        routingKey,
        Buffer.from(JSON.stringify(message)),
        { persistent: true, contentType: "application/json" }
    );
    log(service, "INFO", "topic.message.published", {
        exchange: TOPIC_EXCHANGE,
        routingKey,
        correlationId: message.correlationId,
        messageId: message.messageId,
        published
    });
    return published;
}
