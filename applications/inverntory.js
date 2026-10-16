import express from "express";
const app = express();

app.use(express.json());

const PORT = 8081
const map = new Map();

// Mock some products
map.set("rice",10);
map.set("shampoo",20);
map.set("headphone",3);


export const INV_ORCH_REQUEST_QUEUE = "IV:orchastrator"; 

// curl --json '{"name":"John","role":"admin"}' http://localhost:8081/buy/items
app.post("/buy/items",(req,res)=>{
    console.log(`Request: ${JSON.stringify(req.body)}`);
    var request = req.body;

    let id = request.id;
    let quantity = parseInt(request.quantity);

    console.log(`Request For Item: ${id} with quantity ${quantity}`);
    try{
        reduceInventory(id,quantity);
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
        let id = request['id'];
        let quantity = request['quantity'];
        console.log("Reverting the Operation...");
        increaseInventory(id,quantity); // Increase the reverted 
        res.status(200).json(`{"ItemId":${id} , "Quantity" : ${map.get(id)}}`);
    }
    catch(error){
        res.status(400).json(`{'message' : 'Operation Failed:${error}'}`);
    }
    finally{
        res.send();
    }
});

app.listen(PORT,()=>{
    console.log(`Inventory Service Started Successfully on PORT:${PORT}`);
});


function increaseInventory(itemId,quantity){
    // NOTE: For Now this is only in case of revert
    if(!map.has(itemId)){
        throw new Error(`Item ${itemId} Not Found`);
    }
    map.set(itemId , map.get(itemId) + quantity); // reduce the item
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

