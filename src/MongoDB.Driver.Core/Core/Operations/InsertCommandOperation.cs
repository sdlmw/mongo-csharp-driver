using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core.Operations
{
    /// <summary>
    /// Represents an insert command operation.
    /// </summary>
    /// <seealso cref="MongoDB.Driver.Core.Operations.IWriteOperation{BsonDocument}" />
    /// <seealso cref="MongoDB.Driver.Core.Operations.IRetryableWriteOperation{BsonDocument}" />
    public class InsertCommandOperation : IWriteOperation<BsonDocument>, IRetryableWriteOperation<BsonDocument>
    {
        // private fields
        private bool _bypassDocumentValidation;
        private readonly CollectionNamespace _collectionNamespace;
        private bool _ordered = true;
        private readonly SplittableBatch<BsonDocument> _documents;
        private bool _withRetry;
        private WriteConcern _writeConcern = WriteConcern.Acknowledged;

        // constructors

        // public properties

        // public methods
        /// <inheritdoc />
        public BsonDocument Execute(IWriteBinding binding, CancellationToken cancellationToken)
        {
            using (var context = new RetryableWriteContext(binding, _withRetry))
            {
                return Execute(context, cancellationToken);
            }
        }

        /// <inheritdoc />
        public BsonDocument Execute(RetryableWriteContext context, CancellationToken cancellationToken)
        {
            return RetryableWriteOperationExecutor.Execute(this, context, cancellationToken);
        }

        /// <inheritdoc />
        public async Task<BsonDocument> ExecuteAsync(IWriteBinding binding, CancellationToken cancellationToken)
        {
            using (var context = new RetryableWriteContext(binding, _withRetry))
            {
                return await ExecuteAsync(context, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public Task<BsonDocument> ExecuteAsync(RetryableWriteContext context, CancellationToken cancellationToken)
        {
            return RetryableWriteOperationExecutor.ExecuteAsync(this, context, cancellationToken);
        }

        /// <inheritdoc />
        public BsonDocument ExecuteInitialAttempt(RetryableWriteContext context, long? transactionNumber, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public Task<BsonDocument> ExecuteInitialAttemptAsync(RetryableWriteContext context, long? transactionNumber, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public BsonDocument ExecuteRetry(RetryableWriteContext context, long transactionNumber, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public Task<BsonDocument> ExecuteRetryAsync(RetryableWriteContext context, long transactionNumber, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}
