import assert from "node:assert/strict";
import { createCompensationLedger } from "../../applications/domain/compensationLedger.js";

export function runCompensationLedgerTests() {
    const ledger = createCompensationLedger([["rice", 0]]);

    ledger.record("purchase-1", "rice", 2);
    assert.equal(ledger.rollback("purchase-1", "rice", 2), true);
    assert.equal(ledger.balances.get("rice"), 2);
    const ledger2 = createCompensationLedger([["rice", 0], ["kushal", 0]]);

    assert.equal(ledger2.rollback("missing", "rice", 2), false);
    assert.equal(ledger2.rollback("missing", "kushal", 20), false);
    assert.equal(ledger2.balances.get("rice"), 0);
    assert.equal(ledger2.balances.get("kushal"), 0);
    const ledger3 = createCompensationLedger([["kushal", 0]]);

    ledger3.record("payment-1", "kushal", 20);
    assert.equal(ledger3.rollback("payment-1", "Rohit", 20), false);
    assert.equal(ledger3.rollback("payment-1", "kushal", 10), false);
    assert.equal(ledger3.balances.get("kushal"), 0);
    assert.equal(ledger3.rollback("payment-1", "kushal", 20), true);
    assert.equal(ledger3.rollback("payment-1", "kushal", 20), false);
    assert.equal(ledger3.balances.get("kushal"), 20);
    return 3;
}
