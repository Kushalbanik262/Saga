import express from "express";
import { action, INV_ORCH_REQUEST_QUEUE, INV_ORCH_RESPONSE_QUEUE, postToOrchastrator } from "./commns/orchastratorclient.js";
import { createConnectionAndReturnChannel,postMessage, queueExistsAndConnectionDurable, subscribeToChannel } from "./commns/connections.js";
const app = express();

app.use(express.json());

const PORT = 8081
const map = new Map();

var CONNECTION = null;

// Mock some products
map.set("rice",10);
map.set("shampoo",20);
map.set("headphone",3);


// curl --json '{"name":"John","role":"admin"}' http://localhost:8081/buy/items
app.post("/buy/items",async (req,res)=>{
    console.log(`[Inventory Request] Request: ${JSON.stringify(req.body)}`);
    try{
        // Send Request to the Orchastrator about reducing the stock
        await commandToOrchastrator(req.body,action.ADD);
        //reduceInventory(id,quantity);
        res.status(200).json(`{"ItemId":${id} , "Quantity" : ${map.get(id)}}`);
    }
    catch(error){
        // Issue while redcing
        res.status(400).json(`{'message' : 'Operation Failed:${error}'}`);
    }
    finally{
         res.send();
    }
});


// Buy Any Item
app.post("/rollback",(req,res)=>{
    var request = JSON.stringify(req.body).command; 
    /**
     * "rollback":{
     *  "command":{
     *      "id" : <>,
     *       "quantity" : <>
     *   }
     * }
     * 
     */
    try{
        console.log("Reverting the Operation...");
        increaseInventory(id,quantity); // Increase the reverted 
        res.status(200).json(`{"ItemId":${id} , "Quantity" : ${map.get(id)}}`);
        res.send();
    }
    catch(error){
        res.status(400).json(`{'message' : 'Operation Failed:${error}'}`);
    }
});

app.listen(PORT,()=>{
    console.log(`Inventory Service Started Successfully on PORT:${PORT}`);
});


function increaseInventory(itemId,quantity){
    // NOTE: For Now this is only in case of revert
    if(itemId == null || !map.has(itemId)){
        throw new Error(`Invalid Item ${itemId}`);
    }
    map.set(itemId , map.get(itemId) + quantity); // reduce the item
}


function rollbackAction(request){
    try{
        let id = request['id'];
        let quantity = parseInt(request['quantity']);
        increaseInventory(id,quantity);
    }
    catch(error){
        console.error(`Issue While Rerverting :${error}`);
    }
}


// Affermative Action
async function proceedAction(request){
    // what if falilure in proceed action
    try{
        let id = request.id;
        let quantity = parseInt(request.quantity);
        reduceInventory(id,quantity);
    }
    catch(error){
        // Make Orchastrator aware that Inventory reduction failed
        await commandToOrchastrator(request,action.ROLLBACK);
    }
}


// basically Mocks to Reduce tge inventory
function reduceInventory(itemId,quantity){
    if(itemId == null){
        throw new Error(`ItemId is Null`);
    }
    if(!map.has(itemId)){
        throw new Error(`Item ${itemId} Not Found`);
    }
    // Reduce The Inventory
    if(map.get(itemId) < quantity){
        throw new Error(`Item UnderFlow`);
    }
    console.log(`Reducing Item:${itemId} for ${quantity}`)
    map.set(itemId , map.get(itemId) - quantity); // reduce the item
}

// POST the Reduce Request to the Orchastrator

async function commandToOrchastrator(request,commandType){
    const oReq = {
        "type" : commandType,
        "itemId" : request.id,
        "itemQuantity" : request.quantity
    };

    console.log(`[COMMAND] ${oReq}`)

    if(CONNECTION == null){
        CONNECTION = await createConnectionAndReturnChannel(INV_ORCH_REQUEST_QUEUE);
    }
    await queueExistsAndConnectionDurable(CONNECTION,INV_ORCH_REQUEST_QUEUE); 
    await postToOrchastrator(oReq,INV_ORCH_REQUEST_QUEUE,CONNECTION);
}


async function listnerToOrchResponse(){
    if(CONNECTION == null){
        CONNECTION = await createConnectionAndReturnChannel(INV_ORCH_RESPONSE_QUEUE);
    }
    queueExistsAndConnectionDurable(CONNECTION,INV_ORCH_RESPONSE_QUEUE);

    subscribeToChannel(INV_ORCH_RESPONSE_QUEUE,CONNECTION,async (rawdata)=>{
        const msg = JSON.parse(rawdata.content.toString());
        console.log(`[RECEIVED FROM ACTIVEMQ] Channel/Queue: ${INV_ORCH_REQUEST_QUEUE} Message:${JSON.stringify(msg)}`);
        executeAction(msg);
    });
}



async function executeAction(msg){
    switch(msg.action){
        case "PROCEED": proceedAction(msg);break;
        case "ROLLBACK": rollbackAction(msg);break;
    }
}

listnerToOrchResponse(); // init method -- start listnening the orchastrator


/**
 * Response Message Contract
 * 
 * "action" : "PROCEED" / "ROLLBACK"
 * 
 * 
 * 
 * 
 * 
 */