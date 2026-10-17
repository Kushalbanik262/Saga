
// If will Listen to the Inventory

import { createConnectionAndReturnChannel, postMessage, queueExistsAndConnectionDurable, subscribeToChannel } from "./commns/connections.js";
import { action, INV_ORCH_REQUEST_QUEUE, INV_ORCH_RESPONSE_QUEUE } from "./commns/orchastratorclient.js";

var channel;



async function listenToInventory(){
    if(channel == null){
        channel = await createConnectionAndReturnChannel(INV_ORCH_REQUEST_QUEUE);
    }
    await queueExistsAndConnectionDurable(channel,INV_ORCH_REQUEST_QUEUE);

    await subscribeToChannel(INV_ORCH_REQUEST_QUEUE,channel,async (rawdata)=>{
        const message = JSON.parse(rawdata.content.toString());
        // Identify the message type and proceed accordingly
        console.log(`Message Received From Inventory: ${JSON.stringify(message)}`);
        const msg = message.message;
        console.log(msg);
        //channel.ack(msg);
        if(msg.type == action.ADD){
            // give ProceedAction
            await proceed(INV_ORCH_RESPONSE_QUEUE,msg);
        }
        else{
            await rollback(INV_ORCH_RESPONSE_QUEUE,msg);
        }
    });
} 



async function rollback(queue,msg){
    const commandMessage = {
        "action" : "PROCEED",
        "id": msg.id,
        "quantity" : msg.quantity
    }
    if(channel == null){
        channel = createConnectionAndReturnChannel(queue);
    }
    queueExistsAndConnectionDurable(channel,queue);
    postMessage(channel,commandMessage,queue);
}


async function proceed(queue,msg){
    if(channel == null){
        channel = createConnectionAndReturnChannel(queue);
    }
    queueExistsAndConnectionDurable(channel,queue);
    const commandMessage = {
        "action" : "PROCEED",
        "id": msg.itemId,
        "quantity" : msg.itemQuantity
    }
    postMessage(channel,commandMessage,queue);
}

listenToInventory();