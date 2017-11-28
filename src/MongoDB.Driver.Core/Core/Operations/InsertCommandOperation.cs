/* Copyright 2017 MongoDB Inc.
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

using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.Operations.ElementNameValidators;
using MongoDB.Driver.Core.WireProtocol;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;

namespace MongoDB.Driver.Core.Operations
{
    /// <summary>
    /// Represents an insert command operation.
    /// </summary>
    /// <seealso cref="MongoDB.Driver.Core.Operations.IWriteOperation{BsonDocument}" />
    /// <seealso cref="MongoDB.Driver.Core.Operations.IRetryableWriteOperation{BsonDocument}" />
    public class InsertCommandOperation<TDocument> : IWriteOperation<BsonDocument>, IRetryableWriteOperation<BsonDocument>
    {
        // private fields
        private bool _bypassDocumentValidation;
        private readonly CollectionNamespace _collectionNamespace;
        private readonly SplittableBatch<TDocument> _documents;
        private readonly IBsonSerializer<TDocument> _documentSerializer;
        private readonly MessageEncoderSettings _messageEncoderSettings;
        private bool _ordered = true;
        private bool _retryable;
        private WriteConcern _writeConcern = WriteConcern.Acknowledged;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="InsertCommandOperation{TDocument}"/> class.
        /// </summary>
        /// <param name="collectionNamespace">The collection namespace.</param>
        /// <param name="documents">The documents.</param>
        /// <param name="documentSerializer">The document serializer.</param>
        /// <param name="messageEncoderSettings">The message encoder settings.</param>
        public InsertCommandOperation(
            CollectionNamespace collectionNamespace,
            SplittableBatch<TDocument> documents,
            IBsonSerializer<TDocument> documentSerializer,
            MessageEncoderSettings messageEncoderSettings)
        {
            _collectionNamespace = Ensure.IsNotNull(collectionNamespace, nameof(collectionNamespace));
            _documents = Ensure.IsNotNull(documents, nameof(documents));
            _documentSerializer = Ensure.IsNotNull(documentSerializer, nameof(documentSerializer));
            _messageEncoderSettings = Ensure.IsNotNull(messageEncoderSettings, nameof(messageEncoderSettings));
        }

        // public properties
        /// <summary>
        /// Gets or sets a value indicating whether to bypass document validation.
        /// </summary>
        /// <value>
        /// A value indicating whether to bypass document validation.
        /// </value>
        public bool BypassDocumentValidation
        {
            get { return _bypassDocumentValidation; }
            set { _bypassDocumentValidation = value; }
        }

        /// <summary>
        /// Gets the collection namespace.
        /// </summary>
        /// <value>
        /// The collection namespace.
        /// </value>
        public CollectionNamespace CollectionNamespace
        {
            get { return _collectionNamespace; }
        }

        /// <summary>
        /// Gets the documents.
        /// </summary>
        /// <value>
        /// The documents.
        /// </value>
        public SplittableBatch<TDocument> Documents
        {
            get { return _documents; }
        }

        /// <summary>
        /// Gets the document serializer.
        /// </summary>
        /// <value>
        /// The document serializer.
        /// </value>
        public IBsonSerializer<TDocument> DocumentSerializer
        {
            get { return _documentSerializer; }
        }

        /// <summary>
        /// Gets the message encoder settings.
        /// </summary>
        /// <value>
        /// The message encoder settings.
        /// </value>
        public MessageEncoderSettings MessageEncoderSettings
        {
            get { return _messageEncoderSettings; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the server should process the inserts in order.
        /// </summary>
        /// <value>A value indicating whether the server should process the inserts in order.</value>
        public bool Ordered
        {
            get { return _ordered; }
            set { _ordered = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the operation can be retried.
        /// </summary>
        /// <value>A value indicating whether the operation can be retried.</value>
        public bool Retryable
        {
            get { return _retryable; }
            set { _retryable = value; }
        }

        /// <summary>
        /// Gets or sets the write concern.
        /// </summary>
        /// <value>
        /// The write concern.
        /// </value>
        public WriteConcern WriteConcern
        {
            get { return _writeConcern; }
            set { _writeConcern = Ensure.IsNotNull(value, nameof(value)); }
        }

        // public methods
        /// <inheritdoc />
        public BsonDocument Execute(IWriteBinding binding, CancellationToken cancellationToken)
        {
            using (var context = new RetryableWriteOperationContext(binding, _retryable))
            {
                return RetryableWriteOperationExecutor.Execute(this, context, cancellationToken);
            }
        }

        /// <inheritdoc />
        public async Task<BsonDocument> ExecuteAsync(IWriteBinding binding, CancellationToken cancellationToken)
        {
            using (var context = new RetryableWriteOperationContext(binding, _retryable))
            {
                return await RetryableWriteOperationExecutor.ExecuteAsync(this, context, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        public BsonDocument ExecuteFirstAttempt(RetryableWriteOperationContext context, long? transactionNumber, CancellationToken cancellationToken)
        {
            return ExecuteAttempt(context, transactionNumber, cancellationToken);
        }

        /// <inheritdoc />
        public Task<BsonDocument> ExecuteFirstAttemptAsync(RetryableWriteOperationContext context, long? transactionNumber, CancellationToken cancellationToken)
        {
            return ExecuteAttemptAsync(context, transactionNumber, cancellationToken);
        }

        /// <inheritdoc />
        public BsonDocument ExecuteRetry(RetryableWriteOperationContext context, long transactionNumber, CancellationToken cancellationToken)
        {
            return ExecuteAttempt(context, transactionNumber, cancellationToken);
        }

        /// <inheritdoc />
        public Task<BsonDocument> ExecuteRetryAsync(RetryableWriteOperationContext context, long transactionNumber, CancellationToken cancellationToken)
        {
            return ExecuteAttemptAsync(context, transactionNumber, cancellationToken);
        }

        // private methods
        private BsonDocument CreateCommand(ConnectionDescription connectionDescription, long? transactionNumber)
        {
            var batchSerializer = CreateBatchSerializer(connectionDescription);
            var batchWrapper = new BsonDocumentWrapper(_documents, batchSerializer);
            var command = new BsonDocument
            {
                { "insert", _collectionNamespace.FullName },
                { "ordered", _ordered },
                { "writeConcern", _writeConcern.ToBsonDocument() },
                { "bypassDocumentValidation", _bypassDocumentValidation },
                { "txnNumber", () => transactionNumber.Value, transactionNumber.HasValue },
                { "documents", new BsonArray { batchWrapper } }
            };
            return command;
        }

        private IBsonSerializer<SplittableBatch<BsonDocument>> CreateBatchSerializer(ConnectionDescription connectionDescription)
        {
            var isSystemIndexesCollection = _collectionNamespace.Equals(CollectionNamespace.DatabaseNamespace.SystemIndexesCollection);
            var elementNameValidator = isSystemIndexesCollection ? (IElementNameValidator)NoOpElementNameValidator.Instance : CollectionElementNameValidator.Instance;

            var splitIndex = _documents.SplitIndex;
            if (splitIndex == -1)
            {
                var maxItemSize = connectionDescription.IsMasterResult.MaxDocumentSize;
                var maxBatchSize = connectionDescription.IsMasterResult.MaxMessageSize;
                return new SizeLimitingSplittableBatchSerializer<BsonDocument>(BsonDocumentSerializer.Instance, elementNameValidator, maxItemSize, maxBatchSize);
            }
            else
            {
                var count = splitIndex;
                return new FixedCountSplittableBatchSerializer<BsonDocument>(BsonDocumentSerializer.Instance, elementNameValidator, count);
            }
        }

        private BsonDocument ExecuteAttempt(RetryableWriteOperationContext context, long? transactionNumber, CancellationToken cancellationToken)
        {
            var channel = context.Channel;
            var command = CreateCommand(channel.ConnectionDescription, transactionNumber);
            return channel.Command<BsonDocument>(
                context.ChannelSource.Session,
                ReadPreference.Primary,
                _collectionNamespace.DatabaseNamespace,
                command,
                NoOpElementNameValidator.Instance,
                null, // additionalOptions,
                () => CommandResponseHandling.Return,
                false, // slaveOk
                BsonDocumentSerializer.Instance,
                _messageEncoderSettings,
                cancellationToken);
        }

        private Task<BsonDocument> ExecuteAttemptAsync(RetryableWriteOperationContext context, long? transactionNumber, CancellationToken cancellationToken)
        {
            var channel = context.Channel;
            var command = CreateCommand(channel.ConnectionDescription, transactionNumber);
            return channel.CommandAsync<BsonDocument>(
                context.ChannelSource.Session,
                ReadPreference.Primary,
                _collectionNamespace.DatabaseNamespace,
                command,
                NoOpElementNameValidator.Instance,
                null, // additionalOptions,
                () => CommandResponseHandling.Return,
                false, // slaveOk
                BsonDocumentSerializer.Instance,
                _messageEncoderSettings,
                cancellationToken);
        }
    }
}
