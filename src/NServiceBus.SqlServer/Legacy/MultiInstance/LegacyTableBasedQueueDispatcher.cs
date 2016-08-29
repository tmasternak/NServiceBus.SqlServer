namespace NServiceBus.Transport.SQLServer
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Transactions;

    class LegacyTableBasedQueueDispatcher : IQueueDispatcher
    {
        LegacySqlConnectionFactory connectionFactory;

        public LegacyTableBasedQueueDispatcher(LegacySqlConnectionFactory connectionFactory)
        {
            this.connectionFactory = connectionFactory;
        }

        public virtual async Task DispatchAsIsolated(HashSet<MessageWithAddress> operations, TransportTransaction transportTransaction)
        {
            using (var scope = new TransactionScope(GetScopeOption(transportTransaction), TransactionScopeAsyncFlowOption.Enabled))
            {
                foreach (var operation in operations)
                {
                    var queue = new TableBasedQueue(operation.Address);
                    using (var connection = await connectionFactory.OpenNewConnection(queue.TransportAddress).ConfigureAwait(false))
                    {
                        await queue.Send(operation.Message, connection, null).ConfigureAwait(false);
                    }
                }
                scope.Complete();
            }
        }

        static TransactionScopeOption GetScopeOption(TransportTransaction transportTransaction)
        {
            TransportTransactionMode transactionMode;
            if (!transportTransaction.TryGet(out transactionMode))
            {
                return TransactionScopeOption.Suppress;
            }
            var scopeOption = transactionMode >= TransportTransactionMode.SendsAtomicWithReceive
                ? TransactionScopeOption.RequiresNew
                : TransactionScopeOption.Suppress;
            return scopeOption;
        }

        public virtual async Task DispatchAsNonIsolated(HashSet<MessageWithAddress> operations, TransportTransaction transportTransaction)
        {
            //If dispatch is not isolated then either TS has been created by the receive operation or needs to be created here.
            using (var scope = new TransactionScope(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled))
            {
                foreach (var operation in operations)
                {
                    var queue = new TableBasedQueue(operation.Address);
                    using (var connection = await connectionFactory.OpenNewConnection(queue.TransportAddress).ConfigureAwait(false))
                    {
                        await queue.Send(operation.Message, connection, null).ConfigureAwait(false);
                    }
                }
                scope.Complete();
            }
        }
    }
}