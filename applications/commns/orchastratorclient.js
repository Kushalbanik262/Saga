import { createConnectionAndReturnChannel,postMessage } from "./commns/connections";



async function postToOrchastrator(message, ORCH_REQUEST_QUEUE,CONNECTION_CHANNEL){
    console.log(`[] Posting to Orchastrator with message :${message}`);
    try{
     if(CONNECTION_CHANNEL == null){
         CONNECTION_CHANNEL = await createConnectionAndReturnChannel();
     }
     const msg = {
         "message" : message,
         "time" : Date.now()
     }
     await postMessage(CONNECTION_CHANNEL,msg,ORCH_REQUEST_QUEUE);
  }
  catch(error){}
}
