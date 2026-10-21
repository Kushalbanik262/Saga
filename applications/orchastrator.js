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
    INV_ORCH_REQUEST_ROUTING_KEY,
    PAY_ORCH_REQUEST_QUEUE,
    PAY_ORCH_REQUEST_ROUTING_KEY,
    PAY_ORCH_RESPONSE_QUEUE
} from "./commns/orchastratorclient.js";

const SERVICE = "orchestrator";
let channel;

async function sendInventoryAction(envelope, requestedAction) {
    const message = envelope.message;
    const response = {
        action: requestedAction,
        id: message.itemId,
        quantity: message.itemQuantity,
        userId: message.userId
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
    if (message.userId == null || message.userId === "") {
        throw new Error("Inventory command is missing userId");
    }

    log(SERVICE, "INFO", "inventory.command.processing", {
        correlationId: envelope.correlationId,
        type: message.type,
        itemId: message.itemId,
        quantity: message.itemQuantity,
        userId: message.userId
    });

    if (message.type === action.ADD) await sendInventoryAction(envelope, "PROCEED");
    else if (message.type === action.ROLLBACK) await sendInventoryAction(envelope, "ROLLBACK");
    else throw new Error(`Unsupported inventory command: ${message.type}`);

    if(message.type == action.ADD){
        // Proceed to payment
        // For now Have a mock value for proceeding
       await handlePaymentCommand({
            userId : message.userId,
            amount : message.itemQuantity * 10,
            correlationId : envelope.correlationId,
            type: action.ADD,
            itemId: message.itemId,
            quantity: message.itemQuantity
        });
    }
    if(message.type == action.ROLLBACK){
        // async operation rollback the payment
        await handlePaymentCommand({
            userId : message.userId,
            amount : message.itemQuantity * 10,
            correlationId : envelope.correlationId,
            type: action.ROLLBACK,
            itemId: message.itemId,
            quantity: message.itemQuantity
        });
    }
}

async function listenToInventory() {
    if(channel == null){
        channel = await createConnectionAndReturnChannel(SERVICE);
    }
    await subscribeToTopic(channel, {
        service: SERVICE,
        queue: INV_ORCH_REQUEST_QUEUE,
        routingKeys: [INV_ORCH_REQUEST_ROUTING_KEY],
        handler: handleInventoryCommand
    });
}

async function listenToPayment(){
    if(channel == null){
        channel = await createConnectionAndReturnChannel(SERVICE);
    }
    await subscribeToTopic(channel,{
        service: SERVICE,
        queue: PAY_ORCH_REQUEST_QUEUE,
        routingKeys: [PAY_ORCH_REQUEST_ROUTING_KEY],
        handler: handlePaymentCommand
    });
}


/**
 * So it listens to the payment Responses
 * @param {} envelope 
 */
async function handlePaymentCommand(envelope){
    const message = envelope?.message ?? envelope;
    if (!message?.type) throw new Error("Payment command is missing type");

    log(SERVICE, "INFO", "payment.command.processing", {
        correlationId: envelope.correlationId,
        type: message.type,
        userId: message.userId,
        amount: message.amount,
        // In case of rollback needed information
        itemId: message.itemId,
        quantity: message.quantity ?? message.itemQuantity
    });

    const actionEnvelope = { ...envelope, ...message };

    if (message.type === action.ADD) await sendPaymentAction(actionEnvelope, "PROCEED");
    else if (message.type === action.ROLLBACK) {
        // revert the payment and revert the inventory
        await sendPaymentAction(actionEnvelope, "ROLLBACK");
        await sendInventoryAction({
            ...envelope,
            message: {
                itemId: message.itemId,
                itemQuantity: message.itemQuantity ?? message.quantity,
                userId: message.userId
            }
        }, "ROLLBACK");
    }
    else throw new Error(`Unsupported payment command: ${message.type}`);
}

/**
 * Send Payment Action
 * @param {*} envelope 
 * @param {*} requestedAction 
 */
async function sendPaymentAction(envelope,requestedAction){
    const response = {
        action: requestedAction,
        id: envelope.userId,
        amount: envelope.amount,
        itemId: envelope.itemId,
        quantity: envelope.quantity,
        userId: envelope.userId
    };
    await publishToTopic(
        channel,
        `payment.response.${requestedAction.toLowerCase()}`,
        {
            message: response,
            correlationId: envelope.correlationId,
            messageId: randomUUID(),
            time: new Date().toISOString()
        },
        SERVICE
    );
    log(SERVICE, "INFO", "payment.action.published", {
        correlationId: envelope.correlationId,
        action: requestedAction,
        userId: envelope.userId,
        amount: envelope.amount,
        itemId: envelope.itemId,
        quantity: envelope.quantity
    });
}


// Listen to Inventory and Payment
listenToInventory().catch((error) => {
    log(SERVICE, "ERROR", "topic.listener.failed", { error: errorDetails(error) });
    process.exitCode = 1;
});


listenToPayment().catch((err)=>{
    log(SERVICE,"ERROR","topic.listner.failed", {error: errorDetails(error)});
    process.exitCode = 1;
})
