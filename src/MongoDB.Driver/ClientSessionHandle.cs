﻿/* Copyright 2017-present MongoDB Inc.
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
using MongoDB.Driver.Core.Bindings;

namespace MongoDB.Driver
{
    /// <summary>
    /// A client session handle.
    /// </summary>
    /// <seealso cref="MongoDB.Driver.IClientSessionHandle" />
    internal sealed class ClientSessionHandle : IClientSessionHandle
    {
        // private fields
        private readonly IMongoClient _client;
        private readonly ICoreSessionHandle _coreSession;
        private bool _disposed;
        private readonly ClientSessionOptions _options;
        private readonly IServerSession _serverSession;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="ClientSessionHandle" /> class.
        /// </summary>
        /// <param name="client">The client.</param>
        /// <param name="options">The options.</param>
        /// <param name="coreSession">The wrapped session.</param>
        public ClientSessionHandle(IMongoClient client, ClientSessionOptions options, ICoreSessionHandle coreSession)
        {
            _client = client;
            _options = options;
            _coreSession = coreSession;
            _serverSession = new ServerSession(coreSession.ServerSession);
        }

        // public properties
        /// <inheritdoc />
        public IMongoClient Client => _client;

        /// <inheritdoc />
        public BsonDocument ClusterTime => _coreSession.ClusterTime;

        /// <inheritdoc />
        public ICoreSessionHandle CoreSession => _coreSession;

        /// <inheritdoc />
        public bool IsImplicit => _coreSession.IsImplicit;

        /// <inheritdoc />
        public BsonTimestamp OperationTime => _coreSession.OperationTime;

        /// <inheritdoc />
        public ClientSessionOptions Options => _options;

        /// <inheritdoc />
        public IServerSession ServerSession => _serverSession;

        // public methods
        /// <inheritdoc />
        public void AdvanceClusterTime(BsonDocument newClusterTime)
        {
            _coreSession.AdvanceClusterTime(newClusterTime);
        }

        /// <inheritdoc />
        public void AdvanceOperationTime(BsonTimestamp newOperationTime)
        {
            _coreSession.AdvanceOperationTime(newOperationTime);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (!_disposed)
            {
                _coreSession.Dispose();
                _serverSession.Dispose();
                _disposed = true;
            }
        }

        /// <inheritdoc />
        public IClientSessionHandle Fork()
        {
            return new ClientSessionHandle(_client, _options, _coreSession.Fork());
        }
    }
}
