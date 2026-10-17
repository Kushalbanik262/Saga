import { createConnectionAndReturnChannel,postMessage } from "./connections.js";

export const INV_ORCH_REQUEST_QUEUE = "IV:orchastrator_req"; 
export const INV_ORCH_RESPONSE_QUEUE = "IV:orchastrator_res";



export async function postToOrchastrator(message, ORCH_REQUEST_QUEUE,CONNECTION_CHANNEL){
    console.log(`[] Posting to Orchastrator with message :${JSON.stringify(message)}`);
     if(CONNECTION_CHANNEL == null){
         throw new Error("Invalid Connection Channel");
     }
     const msg = {
         "message" : message,
         "time" : Date.now()
     }
     await postMessage(CONNECTION_CHANNEL,msg,ORCH_REQUEST_QUEUE);
}


export const action = Object.freeze({
    ADD : "add",
    ROLLBACK : "rollback"});