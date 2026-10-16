const app = express();

app.use(express.json());

const PORT = 8082

// For Now Considering only single Account
const account = new Map();
// DUMMY Account For Test
account.set("kushal",100);

export const PAY_ORCH_REQUEST_QUEUE = "IV:orchastrator"; 

// curl --json '{"name":"John","role":"admin"}' http://localhost:8081/buy/items
app.post("/pay/payment",(req,res)=>{
    console.log(`Request: ${JSON.stringify(req.body)}`);
    let request = JSON.stringify(req);
    let id = request['id'];
    let amount = request['amount'];

    try{
        reduceAmount(id,amount);
        res.status(200).json(req.body);
    }
    catch(error){
        res.status(400).json("{'message' : 'Operation Failed'}")
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
    if(!account.has(id)){
        throw new Error(`Amount Not Found for the Id:${id}`);
    }
    if(account.get(id) < amount || amount < 0){
        throw new Error(`Amount Underflow for this Account`);
    }
    account.set(id,account.get(id) - amount); // reduce the amount
}