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
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.WireProtocol;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;

namespace MongoDB.Driver.Core.Operations
{
    /// <summary>
    /// Represents a retryable write command operation.
    /// </summary>
    public abstract class RetryableWriteCommandOperationBase<TResult> : IWriteOperation<TResult>, IRetryableWriteOperation<TResult>
    {
        // private fields
        private readonly DatabaseNamespace _databaseNamespace;
        private readonly MessageEncoderSettings _messageEncoderSettings;
        private readonly IBsonSerializer<TResult> _resultSerializer;
        private bool _retryRequested;
        private WriteConcern _writeConcern = WriteConcern.Acknowledged;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="InsertCommandOperation{TDocument}" /> class.
        /// </summary>
        /// <param name="databaseNamespace">The database namespace.</param>
        /// <param name="resultSerializer">The result serializer.</param>
        /// <param name="messageEncoderSettings">The message encoder settings.</param>
        public RetryableWriteCommandOperationBase(
            DatabaseNamespace databaseNamespace,
            IBsonSerializer<TResult> resultSerializer,
            MessageEncoderSettings messageEncoderSettings)
        {
            _databaseNamespace = Ensure.IsNotNull(databaseNamespace, nameof(databaseNamespace));
            _resultSerializer = Ensure.IsNotNull(resultSerializer, nameof(resultSerializer));
            _messageEncoderSettings = Ensure.IsNotNull(messageEncoderSettings, nameof(messageEncoderSettings));
        }

        // public properties
        /// <summary>
        /// Gets the database namespace.
        /// </summary>
        /// <value>
        /// The database namespace.
        /// </value>
        public DatabaseNamespace DatabaseNamespace
        {
            get { return _databaseNamespace; }
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
        /// Gets the result serializer.
        /// </summary>
        /// <value>
        /// The result serializer.
        /// </value>
        public IBsonSerializer<TResult> ResultSerializer
        {
            get { return _resultSerializer; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether retry is enabled for the operation.
        /// </summary>
        /// <value>A value indicating whether retry is enabled.</value>
        public bool RetryRequested
        {
            get { return _retryRequested; }
            set { _retryRequested = value; }
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
        public virtual TResult Execute(IWriteBinding binding, CancellationToken cancellationToken)
        {
            return RetryableWriteOperationExecutor.Execute(this, binding, _retryRequested, cancellationToken);
        }

        /// <inheritdoc />
        public virtual async Task<TResult> ExecuteAsync(IWriteBinding binding, CancellationToken cancellationToken)
        {
            return await RetryableWriteOperationExecutor.ExecuteAsync(this, binding, _retryRequested, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public TResult ExecuteAttempt(RetryableWriteOperationContext context, int attempt, long? transactionNumber, CancellationToken cancellationToken)
        {
            var channel = context.Channel;
            var command = CreateCommand(channel.ConnectionDescription, attempt, transactionNumber);

            return channel.Command<TResult>(
                context.ChannelSource.Session,
                ReadPreference.Primary,
                _databaseNamespace,
                command,
                NoOpElementNameValidator.Instance,
                null, // additionalOptions,
                () => CommandResponseHandling.Return,
                false, // slaveOk
                _resultSerializer,
                _messageEncoderSettings,
                cancellationToken);
        }

        /// <inheritdoc />
        public Task<TResult> ExecuteAttemptAsync(RetryableWriteOperationContext context, int attempt, long? transactionNumber, CancellationToken cancellationToken)
        {
            var channel = context.Channel;
            var command = CreateCommand(channel.ConnectionDescription, attempt, transactionNumber);

            return channel.CommandAsync<TResult>(
                context.ChannelSource.Session,
                ReadPreference.Primary,
                _databaseNamespace,
                command,
                NoOpElementNameValidator.Instance,
                null, // additionalOptions,
                () => CommandResponseHandling.Return,
                false, // slaveOk
                _resultSerializer,
                _messageEncoderSettings,
                cancellationToken);
        }

        // protected methods
        /// <summary>
        /// Creates the command.
        /// </summary>
        /// <param name="connectionDescription">The connection description.</param>
        /// <param name="attempt">The attempt.</param>
        /// <param name="transactionNumber">The transaction number.</param>
        /// <returns>A command.</returns>
        protected abstract BsonDocument CreateCommand(ConnectionDescription connectionDescription, int attempt, long? transactionNumber);
    }
}
