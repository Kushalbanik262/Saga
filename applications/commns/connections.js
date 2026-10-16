import amqp from 'amqplib';

// Default local connection URL. As noted by users on Stack Overflow, if your 
// credentials contain special characters, make sure they are properly encoded 
// in the connection string format: amqp://user:pass@host:port
const RABBITMQ_URL = 'amqp://admin:SuperSecretPassword123@localhost:5672/%2f'; 
const QUEUE_NAME = 'test_queue';


export async function createConnectionAndReturnChannel(){
        // 1. Establish a connection to the RabbitMQ server
        console.log('Connecting to RabbitMQ...');
        const connection = await amqp.connect(RABBITMQ_URL);
        
        // 2. Create a channel where the messaging API resides
        const channel = await connection.createChannel();
        console.log('Successfully connected and channel created.');

        // 3. Ensure the queue exists (durable: true survives server restarts)
        await channel.assertQueue(QUEUE_NAME, { durable: true });
        return channel;
}



export async function subscribeToChannel(channel,subscription){
    console.log("[] Subsribing to Channel");
    channel.consume(QUEUE_NAME,subscription);
}


/**
 * Post Message to the channel Created
 * @param {*} channel 
 * @param {*} message 
 */
export async function postMessage(channel,text,QUEUE = QUEUE_NAME){
    const message = JSON.stringify({ text: text, timestamp: Date.now() });
    channel.sendToQueue(QUEUE, Buffer.from(message), { persistent: true });
    console.log(`[x] Sent: ${message}`);
}


export async function testPing() {
    try {
    
        const channel = await createConnectionAndReturnChannel();
        postMessage(channel,"Hello RabbitMQ");
        // --- Example: Consuming a Message ---
        console.log('Waiting for messages...');
        await subscribeToChannel(channel,(msg) => {
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

testPing();
