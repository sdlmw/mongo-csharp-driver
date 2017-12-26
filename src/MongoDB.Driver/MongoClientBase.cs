/* Copyright 2010-2017 MongoDB Inc.
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
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core.Clusters;

namespace MongoDB.Driver
{
    /// <summary>
    /// Base class for implementors of <see cref="IMongoClient"/>.
    /// </summary>
    public abstract class MongoClientBase : IMongoClient
    {
        /// <inheritdoc />
        public abstract ICluster Cluster { get; }

        /// <inheritdoc />
        public abstract MongoClientSettings Settings { get; }

        /// <inheritdoc />
        public virtual void DropDatabase(string name, CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public virtual void DropDatabase(IClientSessionHandle session, string name, CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public abstract Task DropDatabaseAsync(string name, CancellationToken cancellationToken = default(CancellationToken));

        /// <inheritdoc />
        public virtual Task DropDatabaseAsync(IClientSessionHandle session, string name, CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public abstract IMongoDatabase GetDatabase(string name, MongoDatabaseSettings settings = null);

        /// <inheritdoc />
        public virtual IEnumerable<string> ListDatabaseNames(
            CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public virtual IEnumerable<string> ListDatabaseNames(
            IClientSessionHandle session,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public virtual Task<IEnumerable<string>> ListDatabaseNamesAsync(
            CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public virtual Task<IEnumerable<string>> ListDatabaseNamesAsync(
            IClientSessionHandle session,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public virtual IAsyncCursor<BsonDocument> ListDatabases(
            ListDatabasesOptions options = null,                                                
            CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public virtual IAsyncCursor<BsonDocument> ListDatabases(
            IClientSessionHandle session, 
            ListDatabasesOptions options = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public abstract Task<IAsyncCursor<BsonDocument>> ListDatabasesAsync(
            ListDatabasesOptions option = null,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <inheritdoc />
        public virtual Task<IAsyncCursor<BsonDocument>> ListDatabasesAsync(
            IClientSessionHandle session, 
            ListDatabasesOptions options = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public virtual IEnumerable<string> ListDatabaseNames()
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public virtual IClientSessionHandle StartSession(ClientSessionOptions options = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public virtual Task<IClientSessionHandle> StartSessionAsync(ClientSessionOptions options = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public virtual IMongoClient WithReadConcern(ReadConcern readConcern)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public virtual IMongoClient WithReadPreference(ReadPreference readPreference)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public virtual IMongoClient WithWriteConcern(WriteConcern writeConcern)
        {
            throw new NotImplementedException();
        }
    }
}
