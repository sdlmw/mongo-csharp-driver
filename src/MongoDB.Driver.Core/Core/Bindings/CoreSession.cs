/* Copyright 2018-present MongoDB Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.Operations;

namespace MongoDB.Driver.Core.Bindings
{
    /// <summary>
    /// Represents a session.
    /// </summary>
    /// <seealso cref="MongoDB.Driver.Core.Bindings.ICoreSession" />
    public sealed class CoreSession : ICoreSession
    {
        // private fields
        private readonly ICluster _cluster;
        private readonly IClusterClock _clusterClock = new ClusterClock();
        private CoreTransaction _currentTransaction;
        private bool _disposed;
        private readonly IOperationClock _operationClock = new OperationClock();
        private readonly CoreSessionOptions _options;
        private readonly ICoreServerSession _serverSession;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="CoreSession" /> class.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="serverSession">The server session.</param>
        /// <param name="options">The options.</param>
        public CoreSession(
            ICluster cluster,
            ICoreServerSession serverSession,
            CoreSessionOptions options)
        {
            _cluster = Ensure.IsNotNull(cluster, nameof(cluster));
            _serverSession = Ensure.IsNotNull(serverSession, nameof(serverSession));
            _options = Ensure.IsNotNull(options, nameof(options));
        }

        // public properties
        /// <inheritdoc />
        public ICluster Cluster => _cluster;

        /// <inheritdoc />
        public BsonDocument ClusterTime => _clusterClock.ClusterTime;

        /// <inheritdoc />
        public CoreTransaction CurrentTransaction => _currentTransaction;

        /// <inheritdoc />
        public BsonDocument Id => _serverSession.Id;

        /// <inheritdoc />
        public bool IsCausallyConsistent => _options.IsCausallyConsistent;

        /// <inheritdoc />
        public bool IsImplicit => _options.IsImplicit;

        /// <inheritdoc />
        public bool IsInTransaction => _currentTransaction != null;

        /// <inheritdoc />
        public BsonTimestamp OperationTime => _operationClock.OperationTime;

        /// <inheritdoc />
        public CoreSessionOptions Options => _options;

        /// <inheritdoc />
        public ICoreServerSession ServerSession => _serverSession;

        // public methods
        /// <inheritdoc />
        public void AbortTransaction(CancellationToken cancellationToken = default(CancellationToken))
        {
            EnsureIsInTransaction(nameof(AbortTransaction));
            var operation = CreateAbortTransactionOperation();
            ExecuteOperationOnPrimary(operation, cancellationToken);
        }

        /// <inheritdoc />
        public Task AbortTransactionAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            EnsureIsInTransaction(nameof(AbortTransactionAsync));
            var operation = CreateAbortTransactionOperation();
            return ExecuteOperationOnPrimaryAsync(operation, cancellationToken);
        }

        /// <inheritdoc />
        public void AdvanceClusterTime(BsonDocument newClusterTime)
        {
            _clusterClock.AdvanceClusterTime(newClusterTime);
        }

        /// <inheritdoc />
        public void AdvanceOperationTime(BsonTimestamp newOperationTime)
        {
            _operationClock.AdvanceOperationTime(newOperationTime);
        }

        /// <inheritdoc />
        public long AdvanceTransactionNumber()
        {
            return _serverSession.AdvanceTransactionNumber();
        }

        /// <inheritdoc />
        public void CommitTransaction(CancellationToken cancellationToken = default(CancellationToken))
        {
            EnsureIsInTransaction(nameof(CommitTransaction));
            var operation = CreateCommitTransactionOperation();
            ExecuteOperationOnPrimary(operation, cancellationToken);
        }

        /// <inheritdoc />
        public Task CommitTransactionAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            EnsureIsInTransaction(nameof(CommitTransactionAsync));
            var operation = CreateCommitTransactionOperation();
            return ExecuteOperationOnPrimaryAsync(operation, cancellationToken);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (!_disposed)
            {
                _serverSession.Dispose();
                _disposed = true;
            }
        }

        /// <inheritdoc />
        public void StartTransaction(TransactionOptions transactionOptions = null)
        {
            if (_currentTransaction != null)
            {
                throw new InvalidOperationException("StartTransaction cannot be called when the session is already in a transaction.");
            }

            var transactionNumber = AdvanceTransactionNumber();
            var readConcern = transactionOptions?.ReadConcern ?? _options.DefaultTransactionOptions?.ReadConcern ?? ReadConcern.Snapshot;
            var writeConcern = transactionOptions?.WriteConcern ?? _options.DefaultTransactionOptions?.WriteConcern ?? WriteConcern.WMajority;
            var effectiveTransactionOptions = new TransactionOptions(readConcern, writeConcern);
            var transaction = new CoreTransaction(transactionNumber, effectiveTransactionOptions);

            _currentTransaction = transaction;
        }

        /// <inheritdoc />
        public void WasUsed()
        {
            _serverSession.WasUsed();
        }

        // private methods
        private IReadOperation<BsonDocument> CreateAbortTransactionOperation()
        {
            return new AbortTransactionOperation(GetTransactionWriteConcern());
        }

        private IReadOperation<BsonDocument> CreateCommitTransactionOperation()
        {
            return new CommitTransactionOperation(GetTransactionWriteConcern());
        }

        private void EnsureIsInTransaction(string methodName)
        {
            if (_currentTransaction == null)
            {
                throw new InvalidOperationException("${methodName} can only be called when the session is in a transaction.");
            }
        }

        private TResult ExecuteOperationOnPrimary<TResult>(IReadOperation<TResult> operation, CancellationToken cancellationToken)
        {
            using (var sessionHandle = new CoreSessionHandle(this))
            using (var binding = new WritableServerBinding(_cluster, sessionHandle))
            {
                return operation.Execute(binding, cancellationToken);
            }
        }

        private async Task<TResult> ExecuteOperationOnPrimaryAsync<TResult>(IReadOperation<TResult> operation, CancellationToken cancellationToken)
        {
            using (var sessionHandle = new CoreSessionHandle(this))
            using (var binding = new WritableServerBinding(_cluster, sessionHandle))
            {
                return await operation.ExecuteAsync(binding, cancellationToken).ConfigureAwait(false);
            }
        }

        private WriteConcern GetTransactionWriteConcern()
        {
            return
                _currentTransaction.TransactionOptions.WriteConcern ??
                _options.DefaultTransactionOptions?.WriteConcern ??
                WriteConcern.WMajority;
        }
    }
}
