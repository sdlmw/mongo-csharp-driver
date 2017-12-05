/* Copyright 2013-2017 MongoDB Inc.
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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;

namespace MongoDB.Driver.Core.Operations
{
    /// <summary>
    /// Represents an insert operation using the insert opcode.
    /// </summary>
    /// <typeparam name="TDocument">The type of the document.</typeparam>
    public class InsertOpcodeOperation<TDocument> : IWriteOperation<IEnumerable<WriteConcernResult>>
    {
        // fields
        private bool? _bypassDocumentValidation;
        private readonly CollectionNamespace _collectionNamespace;
        private bool _continueOnError;
        private readonly IReadOnlyList<TDocument> _documents;
        private int? _maxBatchCount;
        private int? _maxDocumentSize;
        private int? _maxMessageSize;
        private readonly MessageEncoderSettings _messageEncoderSettings;
        private readonly IBsonSerializer<TDocument> _serializer;
        private WriteConcern _writeConcern;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="InsertOpcodeOperation{TDocument}"/> class.
        /// </summary>
        /// <param name="collectionNamespace">The collection namespace.</param>
        /// <param name="documents">The documents.</param>
        /// <param name="serializer">The serializer.</param>
        /// <param name="messageEncoderSettings">The message encoder settings.</param>
        public InsertOpcodeOperation(CollectionNamespace collectionNamespace, IEnumerable<TDocument> documents, IBsonSerializer<TDocument> serializer, MessageEncoderSettings messageEncoderSettings)
        {
            _collectionNamespace = Ensure.IsNotNull(collectionNamespace, nameof(collectionNamespace));
            _documents = Ensure.IsNotNull(documents, nameof(documents)).ToList();
            _serializer = Ensure.IsNotNull(serializer, nameof(serializer));
            _messageEncoderSettings = Ensure.IsNotNull(messageEncoderSettings, nameof(messageEncoderSettings));
            _writeConcern = WriteConcern.Acknowledged;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InsertOpcodeOperation{TDocument}"/> class.
        /// </summary>
        /// <param name="collectionNamespace">The collection namespace.</param>
        /// <param name="documentSource">The document source.</param>
        /// <param name="serializer">The serializer.</param>
        /// <param name="messageEncoderSettings">The message encoder settings.</param>
        public InsertOpcodeOperation(CollectionNamespace collectionNamespace, BatchableSource<TDocument> documentSource, IBsonSerializer<TDocument> serializer, MessageEncoderSettings messageEncoderSettings)
            : this(collectionNamespace, Ensure.IsNotNull(documentSource, nameof(documentSource)).Items.Skip(documentSource.Offset).Take(documentSource.Count), serializer, messageEncoderSettings)
        {
        }

        // properties
        /// <summary>
        /// Gets or sets a value indicating whether to bypass document validation.
        /// </summary>
        /// <value>
        /// A value indicating whether to bypass document validation.
        /// </value>
        public bool? BypassDocumentValidation
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
        /// Gets a value indicating whether the server should continue on error.
        /// </summary>
        /// <value>
        ///   <c>true</c> if the server should continue on error; otherwise, <c>false</c>.
        /// </value>
        public bool ContinueOnError
        {
            get { return _continueOnError; }
            set { _continueOnError = value; }
        }

        /// <summary>
        /// Gets the document source.
        /// </summary>
        /// <value>
        /// The document source.
        /// </value>
        public IEnumerable<TDocument> Documents
        {
            get { return _documents; }
        }

        /// <summary>
        /// Gets the document source.
        /// </summary>
        /// <value>
        /// The document source.
        /// </value>
        [Obsolete("Use Documents instead.")]
        public BatchableSource<TDocument> DocumentSource
        {
            get { return new BatchableSource<TDocument>(_documents); }
        }

        /// <summary>
        /// Gets or sets the maximum number of documents in a batch.
        /// </summary>
        /// <value>
        /// The maximum number of documents in a batch.
        /// </value>
        public int? MaxBatchCount
        {
            get { return _maxBatchCount; }
            set { _maxBatchCount = Ensure.IsNullOrGreaterThanZero(value, nameof(value)); }
        }

        /// <summary>
        /// Gets or sets the maximum size of a document.
        /// </summary>
        /// <value>
        /// The maximum size of a document.
        /// </value>
        public int? MaxDocumentSize
        {
            get { return _maxDocumentSize; }
            set { _maxDocumentSize = Ensure.IsNullOrGreaterThanZero(value, nameof(value)); }
        }

        /// <summary>
        /// Gets or sets the maximum size of a message.
        /// </summary>
        /// <value>
        /// The maximum size of a message.
        /// </value>
        public int? MaxMessageSize
        {
            get { return _maxMessageSize; }
            set { _maxMessageSize = Ensure.IsNullOrGreaterThanZero(value, nameof(value)); }
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
        /// Gets the serializer.
        /// </summary>
        /// <value>
        /// The serializer.
        /// </value>
        public IBsonSerializer<TDocument> Serializer
        {
            get { return _serializer; }
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
        /// <inheritdoc/>
        public IEnumerable<WriteConcernResult> Execute(IWriteBinding binding, CancellationToken cancellationToken)
        {
            Ensure.IsNotNull(binding, nameof(binding));

            using (EventContext.BeginOperation())
            using (var context = RetryableWriteContext.Create(binding, false, cancellationToken))
            {
                return Execute(context, cancellationToken);
            }
        }

        /// <inheritdoc/>
        public IEnumerable<WriteConcernResult> Execute(RetryableWriteContext context, CancellationToken cancellationToken)
        {
            if (Feature.WriteCommands.IsSupported(context.Channel.ConnectionDescription.ServerVersion) && _writeConcern.IsAcknowledged)
            {
                var emulator = CreateEmulator();
                var result = emulator.Execute(context, cancellationToken);
                return new[] { result };
            }
            else
            {
                return InsertMultipleBatches(context.Channel, cancellationToken);
            }
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<WriteConcernResult>> ExecuteAsync(IWriteBinding binding, CancellationToken cancellationToken)
        {
            Ensure.IsNotNull(binding, nameof(binding));

            using (EventContext.BeginOperation())
            using (var context = await RetryableWriteContext.CreateAsync(binding, false, cancellationToken).ConfigureAwait(false))
            {
                return await ExecuteAsync(context, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<WriteConcernResult>> ExecuteAsync(RetryableWriteContext context, CancellationToken cancellationToken)
        {
            if (Feature.WriteCommands.IsSupported(context.Channel.ConnectionDescription.ServerVersion) && _writeConcern.IsAcknowledged)
            {
                var emulator = CreateEmulator();
                var result = await emulator.ExecuteAsync(context, cancellationToken).ConfigureAwait(false);
                return new[] { result };
            }
            else
            {
                return await InsertMultipleBatchesAsync(context.Channel, cancellationToken).ConfigureAwait(false);
            }
        }

        // private methods
        private InsertOpcodeOperationEmulator<TDocument> CreateEmulator()
        {
            return new InsertOpcodeOperationEmulator<TDocument>(_collectionNamespace, _serializer, _documents, _messageEncoderSettings)
            {
                BypassDocumentValidation = _bypassDocumentValidation,
                ContinueOnError = _continueOnError,
                MaxBatchCount = _maxBatchCount,
                MaxDocumentSize = _maxDocumentSize,
                MaxMessageSize = _maxMessageSize,
                WriteConcern = _writeConcern
            };
        }

        private WriteConcernResult ExecuteProtocol(IChannelHandle channel, Batch batch, CancellationToken cancellationToken)
        {
            return channel.Insert<TDocument>(
                _collectionNamespace,
                batch.WriteConcern,
                _serializer,
                _messageEncoderSettings,
                batch.Documents,
                _maxBatchCount,
                _maxMessageSize,
                _continueOnError,
                batch.ShouldSendGetLastError,
                cancellationToken);
        }

        private Task<WriteConcernResult> ExecuteProtocolAsync(IChannelHandle channel, Batch batch, CancellationToken cancellationToken)
        {
            return channel.InsertAsync<TDocument>(
                _collectionNamespace,
                batch.WriteConcern,
                _serializer,
                _messageEncoderSettings,
                batch.Documents,
                _maxBatchCount,
                _maxMessageSize,
                _continueOnError,
                batch.ShouldSendGetLastError,
                cancellationToken);
        }

        private IEnumerable<WriteConcernResult> InsertMultipleBatches(IChannelHandle channel, CancellationToken cancellationToken)
        {
            var helper = new BatchHelper(_documents, _writeConcern, _continueOnError);
            foreach (var batch in helper.GetBatches())
            {
                try
                {
                    batch.Result = ExecuteProtocol(channel, batch, cancellationToken);
                }
                catch (MongoWriteConcernException ex)
                {
                    batch.Result = helper.HandleException(ex);
                    if (!_continueOnError)
                    {
                        return null;
                    }
                }
            }

            return helper.CreateFinalResultOrThrow();
        }

        private async Task<IEnumerable<WriteConcernResult>> InsertMultipleBatchesAsync(IChannelHandle channel, CancellationToken cancellationToken)
        {
            var helper = new BatchHelper(_documents, _writeConcern, _continueOnError);
            foreach (var batch in helper.GetBatches())
            {
                try
                {
                    batch.Result = await ExecuteProtocolAsync(channel, batch, cancellationToken).ConfigureAwait(false);
                }
                catch (MongoWriteConcernException ex)
                {
                    batch.Result = helper.HandleException(ex);
                    if (!_continueOnError)
                    {
                        return null;
                    }
                }
            }

            return helper.CreateFinalResultOrThrow();
        }

        // nested types
        private class Batch
        {
            public BatchableSource<TDocument> Documents;
            public WriteConcernResult Result;
            public Func<bool> ShouldSendGetLastError;
            public WriteConcern WriteConcern;
        }

        private class BatchHelper
        {
            // private fields
            private readonly bool _continueOnError;
            private readonly IReadOnlyList<TDocument> _documents;
            private Exception _finalException;
            private bool _hasWriteErrors;
            private readonly List<WriteConcernResult> _results = new List<WriteConcernResult>();
            private readonly WriteConcern _writeConcern;

            // constructors
            public BatchHelper(IReadOnlyList<TDocument> documents, WriteConcern writeConcern, bool continueOnError)
            {
                _documents = documents;
                _writeConcern = writeConcern;
                _continueOnError = continueOnError;
            }

            // public methods
            public IEnumerable<Batch> GetBatches()
            {
                var documents = new BatchableSource<TDocument>(_documents, 0, _documents.Count, canBeAdjusted: true);
                while (documents.Count > 0 && ShouldContinue())
                {
                    var writeConcern = _writeConcern;
                    if (!writeConcern.IsAcknowledged)
                    {
                        writeConcern = WriteConcern.W1;
                    }

                    var batch = new Batch
                    {
                        Documents = documents,
                        WriteConcern = writeConcern,
                        ShouldSendGetLastError = () => documents.Count > 0
                    };

                    yield return batch;

                    _results.Add(batch.Result);
                    _hasWriteErrors |= HasWriteErrors(batch.Result);
                    documents.AdvanceOverAdjustedCount();
                }
            }

            public WriteConcernResult HandleException(MongoWriteConcernException exception)
            {
                var result = exception.WriteConcernResult;

                if (_continueOnError)
                {
                    _finalException = exception;
                }
                else if (_writeConcern.IsAcknowledged)
                {
                    _results.Add(result);
                    exception.Data["results"] = _results;
                    throw exception;
                }

                return result;
            }

            public IEnumerable<WriteConcernResult> CreateFinalResultOrThrow()
            {
                if (_writeConcern.IsAcknowledged && _finalException != null)
                {
                    _finalException.Data["results"] = _results;
                    throw _finalException;
                }

                return _results;
            }

            // private methods
            private bool HasWriteErrors(WriteConcernResult result)
            {
                throw new NotImplementedException();
            }

            private bool ShouldContinue()
            {
                return !_hasWriteErrors || _continueOnError;
            }
        }
    }
}
