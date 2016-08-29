namespace NServiceBus.Transport.SQLServer
{
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using System.Transactions;
    using Transport;

    class TableBasedQueueDispatcher : IQueueDispatcher
    {
        SqlConnectionFactory connectionFactory;

        public TableBasedQueueDispatcher(SqlConnectionFactory connectionFactory)
        {
            this.connectionFactory = connectionFactory;
        }

        public async Task DispatchAsIsolated(HashSet<MessageWithAddress> operations, TransportTransaction transportTransaction)
        {
            if (operations.Count == 0)
            {
                return;
            }
            using (var scope = new TransactionScope(GetScopeOption(transportTransaction), TransactionScopeAsyncFlowOption.Enabled))
            using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
            {
                await Send(operations, connection, null).ConfigureAwait(false);

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

        public async Task DispatchAsNonIsolated(HashSet<MessageWithAddress> operations, TransportTransaction transportTransaction)
        {
            if (operations.Count == 0)
            {
                return;
            }

            if (UseReceiveTransaction(transportTransaction))
            {
                await DispatchUsingReceiveTransaction(transportTransaction, operations).ConfigureAwait(false);
            }
            else
            {
                await DispatchOperationsWithNewConnectionAndTransaction(operations).ConfigureAwait(false);
            }
        }

        static bool UseReceiveTransaction(TransportTransaction transportTransaction)
        {
            TransportTransactionMode transactionMode;
            if (!transportTransaction.TryGet(out transactionMode))
            {
                return false;
            }
            return transactionMode >= TransportTransactionMode.SendsAtomicWithReceive;
        }


        async Task DispatchOperationsWithNewConnectionAndTransaction(HashSet<MessageWithAddress> operations)
        {
            using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
            {
                if (operations.Count == 1)
                {
                    await Send(operations, connection, null).ConfigureAwait(false);
                    return;
                }

                using (var transaction = connection.BeginTransaction())
                {
                    await Send(operations, connection, transaction).ConfigureAwait(false);
                    transaction.Commit();
                }
            }
        }

        async Task DispatchUsingReceiveTransaction(TransportTransaction transportTransaction, HashSet<MessageWithAddress> operations)
        {
            SqlConnection sqlTransportConnection;
            SqlTransaction sqlTransportTransaction;
            Transaction ambientTransaction;

            transportTransaction.TryGet(out sqlTransportConnection);
            transportTransaction.TryGet(out sqlTransportTransaction);
            transportTransaction.TryGet(out ambientTransaction);

            if (ambientTransaction != null)
            {
                using (var connection = await connectionFactory.OpenNewConnection().ConfigureAwait(false))
                {
                    await Send(operations, connection, null).ConfigureAwait(false);
                }
            }
            else
            {
                await Send(operations, sqlTransportConnection, sqlTransportTransaction).ConfigureAwait(false);
            }
        }

        static async Task Send(HashSet<MessageWithAddress> operations, SqlConnection connection, SqlTransaction transaction)
        {
            foreach (var operation in operations)
            {
                var queue = new TableBasedQueue(operation.Address);
                await queue.Send(operation.Message, connection, transaction).ConfigureAwait(false);
            }
        }
    }
}