import { runCompensationLedgerTests } from "./unit/compensationLedger.test.js";
import { runRabbitmqRoutingTest } from "./integration/rabbitmqRouting.test.js";

const unitCount = runCompensationLedgerTests();
console.log(`PASS unit tests: ${unitCount}`);

const integration = await runRabbitmqRoutingTest();
if (integration.skipped) {
    console.log("SKIP integration test: RabbitMQ is not running");
} else {
    console.log("PASS integration test: payment rollback routing");
}

console.log("All configured tests passed");
