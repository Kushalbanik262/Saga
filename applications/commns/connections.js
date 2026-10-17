import amqp from 'amqplib';

// Default local connection URL. As noted by users on Stack Overflow, if your 
// credentials contain special characters, make sure they are properly encoded 
// in the connection string format: amqp://user:pass@host:port
const RABBITMQ_URL = 'amqp://admin:SuperSecretPassword123@localhost:5672/%2f'; 
const TEST_QUEUE_NAME = 'test_queue';


/**
 * Makes Sure the Queue exists and Connection is Durable
 * @param {*} channel 
 * @param {*} queueName 
 * @returns 
 */
export async function queueExistsAndConnectionDurable(channel,queueName){
        // 3. Ensure the queue exists (durable: true survives server restarts)
        await channel.assertQueue(queueName, { durable: true });
        return channel;
}


/**
 * Caller Expected to have the sequence to first createConnectionAndReturnChannel and then queueExistsAndConnectionDurable
 */

export async function createConnectionAndReturnChannel(QUEUE_NAME = TEST_QUEUE_NAME){
        // 1. Establish a connection to the RabbitMQ server
        console.log('Connecting to RabbitMQ...');
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();
        console.log('Successfully connected and channel created.');
        return await queueExistsAndConnectionDurable(channel,QUEUE_NAME);
}



export async function subscribeToChannel(QUEUE,channel,subscription){
    console.log(`[CHANNEL SUBSCRIPTION] Subsribing to Channel for QUEUE:${QUEUE}`);
    channel.consume(QUEUE,subscription);
}


/**
 * Post Message to the channel Created
 * @param {*} channel 
 * @param {*} message 
 */
export async function postMessage(channel,text,QUEUE = TEST_QUEUE_NAME){
    const message = JSON.stringify(text);
    channel.sendToQueue(QUEUE, Buffer.from(message), { persistent: true });
    console.log(`[x] Sent: ${message}`);
}


export async function testPing() {
    try {
        const channel = await createConnectionAndReturnChannel();
        postMessage(channel,"Hello RabbitMQ");
        // --- Example: Consuming a Message ---
        console.log('Waiting for messages...');
        await subscribeToChannel(TEST_QUEUE_NAME,channel,(msg) => {
            if (msg !== null) {
                console.log(`[o] Received: ${msg.content.toString()}`);
                
                // Acknowledge receipt of the message
                channel.ack(msg);
            }
        });

    } catch (error) {
        console.error('Error connecting to RabbitMQ:', error);
    }
}
