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

using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;

namespace MongoDB.Driver.Core.Operations
{
    /// <summary>
    /// Represents an update command operation.
    /// </summary>
    public class UpdateCommandOperation<TDocument> : RetryableWriteCommandOperationBase<BsonDocument>
    {
        // private fields
        private bool _bypassDocumentValidation;
        private readonly CollectionNamespace _collectionNamespace;
        private readonly SplittableBatch<UpdateRequest> _updates;
        private bool _ordered = true;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="InsertCommandOperation{TDocument}" /> class.
        /// </summary>
        /// <param name="collectionNamespace">The collection namespace.</param>
        /// <param name="updates">The updates.</param>
        /// <param name="messageEncoderSettings">The message encoder settings.</param>
        public UpdateCommandOperation(
            CollectionNamespace collectionNamespace,
            SplittableBatch<UpdateRequest> updates,
            MessageEncoderSettings messageEncoderSettings)
            : base(Ensure.IsNotNull(collectionNamespace, nameof(collectionNamespace)).DatabaseNamespace, BsonDocumentSerializer.Instance, messageEncoderSettings)
        {
            _collectionNamespace = Ensure.IsNotNull(collectionNamespace, nameof(collectionNamespace));
            _updates = Ensure.IsNotNull(updates, nameof(updates));
        }

        // public properties
        /// <summary>
        /// Gets or sets a value indicating whether to bypass document validation.
        /// </summary>
        /// <value>A value indicating whether to bypass document validation.</value>
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
        /// Gets or sets a value indicating whether the server should process the inserts in order.
        /// </summary>
        /// <value>A value indicating whether the server should process the inserts in order.</value>
        public bool Ordered
        {
            get { return _ordered; }
            set { _ordered = value; }
        }

        /// <summary>
        /// Gets the updates.
        /// </summary>
        /// <value>
        /// The updates.
        /// </value>
        public SplittableBatch<UpdateRequest> Updates
        {
            get { return _updates; }
        }

        // protected methods
        /// <inheritdoc />
        protected override BsonDocument CreateCommand(ConnectionDescription connectionDescription, int attempt, long? transactionNumber)
        {
            var batchSerializer = CreateBatchSerializer(connectionDescription, attempt);
            var batchWrapper = new BsonDocumentWrapper(_updates, batchSerializer);

            return new BsonDocument
            {
                { "update", _collectionNamespace.CollectionName },
                { "ordered", _ordered },
                { "writeConcern", WriteConcern.ToBsonDocument() },
                { "bypassDocumentValidation", _bypassDocumentValidation },
                { "txnNumber", () => transactionNumber.Value, transactionNumber.HasValue },
                { "updates", new BsonArray { batchWrapper } }
            };
        }

        // private methods
        private IBsonSerializer<SplittableBatch<UpdateRequest>> CreateBatchSerializer(ConnectionDescription connectionDescription, int attempt)
        {
            if (attempt == 1)
            {
                var maxItemSize = connectionDescription.IsMasterResult.MaxDocumentSize;
                var maxBatchSize = connectionDescription.IsMasterResult.MaxMessageSize;
                return new SizeLimitingSplittableBatchSerializer<UpdateRequest>(UpdateRequestSerializer.Instance, NoOpElementNameValidator.Instance, maxItemSize, maxBatchSize);
            }
            else
            {
                var count = _updates.SplitIndex;
                return new FixedCountSplittableBatchSerializer<UpdateRequest>(UpdateRequestSerializer.Instance, NoOpElementNameValidator.Instance, count);
            }
        }

        // nested types
        private class UpdateRequestSerializer : SealedClassSerializerBase<UpdateRequest>
        {
            public static readonly IBsonSerializer<UpdateRequest> Instance = new UpdateRequestSerializer();

            protected override void SerializeValue(BsonSerializationContext context, BsonSerializationArgs args, UpdateRequest value)
            {
                var writer = context.Writer;
                writer.WriteStartDocument();
                writer.WriteName("q");
                BsonDocumentSerializer.Instance.Serialize(context, value.Filter);
                writer.WriteName("u");
                BsonDocumentSerializer.Instance.Serialize(context, value.Update);
                writer.WriteName("upsert");
                writer.WriteBoolean(value.IsUpsert);
                writer.WriteName("multi");
                writer.WriteBoolean(value.IsMulti);
                if (value.Collation != null)
                {
                    BsonDocumentSerializer.Instance.Serialize(context, value.Collation.ToBsonDocument());
                }
                if (value.ArrayFilters != null)
                {
                    writer.WriteName("arrayFilters");
                    foreach (var arrayFilter in value.ArrayFilters)
                    {
                        BsonDocumentSerializer.Instance.Serialize(context, arrayFilter);
                    }
                    writer.WriteEndArray();
                }
                writer.WriteEndDocument();
            }
        }
    }
}
