export function createCompensationLedger(initialBalances) {
    const balances = new Map(initialBalances);
    const transactions = new Map();

    function record(transactionId, entityId, amount) {
        transactions.set(transactionId, { entityId, amount });
    }

    function rollback(transactionId, entityId, amount) {
        const transaction = transactions.get(transactionId);
        if (
            transaction == null ||
            transaction.entityId !== entityId ||
            transaction.amount !== amount
        ) {
            return false;
        }

        balances.set(entityId, balances.get(entityId) + amount);
        transactions.delete(transactionId);
        return true;
    }

    return { balances, transactions, record, rollback };
}
