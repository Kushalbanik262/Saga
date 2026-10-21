import express from "express";
import { createConnectionAndReturnChannel, errorDetails, log, subscribeToTopic } from "./commns/connections.js";
import { action, PAY_ORCH_RESPONSE_QUEUE, PAY_ORCH_RESPONSE_ROUTING_KEY, postToOrchastrator } from "./commns/orchastratorclient.js";
const app = express();
app.use(express.json());

const PORT = 8082
var channel;

const SERVICE = "payment";

// For Now Considering only single Account
const account = new Map();
// DUMMY Account For Test
account.set("kushal",0);
account.set("Rohit",0);
// A payment rollback is valid only for a debit that succeeded.
const paymentTransactions = new Map();



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
    if(id == null || !Number.isFinite(Number(amount)) || Number(amount) < 0){
        throw new Error(`Id is Null or amount is invalid`);
    }
    if(!account.has(id)){
        throw new Error(`Amount Not Found for the Id:${id}`);
    }
    if(account.get(id) < amount || amount < 0){
        throw new Error(`Amount Underflow for this Account`);
    }
    console.log(`Reducing Amount for : ${id}`);
    account.set(id,account.get(id) - Number(amount)); // reduce the amount
    return account.get(id);
}



// Command to Orchastrator
async function commandToOrchastrator(request,commandType){
    const command = {
        type: commandType,
        itemId: request.itemId,
        itemQuantity: request.quantity,
        userId: request.userId,
        amount: request.amount
    };

    log(SERVICE,"INFO","payment.orchastrator.command",command);


    if(channel == null){
        channel = await createConnectionAndReturnChannel(SERVICE);
    }

    await postToOrchastrator(command,channel,SERVICE); // Post to orchastrator directly
}



async function listenToOrchestrator() {
    if (channel == null){ channel = await createConnectionAndReturnChannel(SERVICE);}
    await subscribeToTopic(channel, {
        service: SERVICE,
        queue: PAY_ORCH_RESPONSE_QUEUE,
        routingKeys: [PAY_ORCH_RESPONSE_ROUTING_KEY],
        handler: executeAction
    });
}


async function executeAction(envelope){
    const message = envelope.message ?? envelope;
    const { action: requestedAction, userId, amount } = message;
    if (userId == null || userId === "") {
        throw new Error("Payment action is missing userId");
    }
    log(SERVICE, "INFO", "payment.action.started", {
        correlationId: envelope.correlationId, action: requestedAction, userId: userId, amount
    });
    
        if (requestedAction === "PROCEED") {
            try {
                const remainingAmount = reduceAmount(userId, amount);
                paymentTransactions.set(envelope.correlationId, {
                    userId,
                    amount: Number(amount)
                });
                log(SERVICE, "INFO", "payment.reduction.completed", {
                    correlationId: envelope.correlationId, action: requestedAction,userId: userId, amount, remainingAmount
                });
            } catch (error) {
                log(SERVICE, "ERROR", "payment.reduction.failed", {
                    correlationId: envelope.correlationId, action: requestedAction,userId, amount,error: errorDetails(error)
                });
                await commandToOrchastrator({ ...(envelope.message ?? envelope), correlationId: envelope.correlationId }, action.ROLLBACK);
            }
        } else if (requestedAction === "ROLLBACK") {
            if (!rollbackPayment(envelope.correlationId, userId, amount)) {
                log(SERVICE, "WARN", "payment.rollback.ignored", {
                    correlationId: envelope.correlationId,
                    userId,
                    amount,
                    reason: "No successful payment transaction found"
                });
                return;
            }
            log(SERVICE, "INFO", "payment.rollback.completed", {
                correlationId: envelope.correlationId, userId, amount
            });
        } else {
            throw new Error(`Unsupported Payment action: ${requestedAction}`);
      }
}

function rollbackPayment(correlationId, userId, amount) {
    const transaction = paymentTransactions.get(correlationId);
    const parsedAmount = Number(amount);

    if (
        transaction == null ||
        transaction.userId !== userId ||
        transaction.amount !== parsedAmount
    ) {
        return false;
    }

    addAmount(userId, parsedAmount);
    paymentTransactions.delete(correlationId);
    return true;
}



app.listen(PORT,()=>{
    console.log(`Payment Service Started Successfuly on PORT:${PORT}`);
});

// Start Listning to Orchastrator
listenToOrchestrator().catch((err)=>{
    log(SERVICE,"ERROR","topic.listener.failed", { error: errorDetails(err) });
    process.exit(1);
});
