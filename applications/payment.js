import express from "express";
const app = express();
app.use(express.json());

const PORT = 8082

// For Now Considering only single Account
const account = new Map();
// DUMMY Account For Test
account.set("kushal",100);
account.set("Rohit",200);


export const PAY_ORCH_REQUEST_QUEUE = "IV:orchastrator"; 

// curl --json '{"id":"John","amount":20}' http://localhost:8081/buy/items
app.post("/pay/payment",(req,res)=>{
    console.log(`Request: ${JSON.stringify(req.body)}`);

    let request = req.body;
    let id = request.id;
    let amount = parseInt(request.amount);

    console.log(`Request For Payment:${id} and amount ${amount}`)

    try{
        reduceAmount(id,amount);
        res.status(200).json(`{"message" : "Fetched Successfully" , "id" : ${id} , "amount" : "***" }"`);
    }
    catch(error){
        res.status(400).json(`{'message' : 'Operation Failed:${error}'}`)
    }
    finally{
        res.send();
    }
});



function addAmount(id,amount){
    // Only in case of Revert for now
    if(!account.has(id)){
        throw new Error(`Amount Not Found for the Id:${id}`);
    }

    account.set(id,account.get(id) + amount);
}   


function reduceAmount(id,amount){
    if(id == null || amount == NaN || amount < 0){
        throw new Error(`Id is Null or amount is invalid`);
    }
    if(!account.has(id)){
        throw new Error(`Amount Not Found for the Id:${id}`);
    }
    if(account.get(id) < amount || amount < 0){
        throw new Error(`Amount Underflow for this Account`);
    }
    console.log(`Reducing Amount for : ${id}`);
    account.set(id,account.get(id) - amount); // reduce the amount
}

app.listen(PORT,()=>{
    console.log(`Payment Service Started Successfuly on PORT:${PORT}`);
});