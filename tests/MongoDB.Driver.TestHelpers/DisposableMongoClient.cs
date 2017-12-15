﻿/* Copyright 2017 MongoDB Inc.
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

namespace MongoDB.Driver.TestHelpers
{
    public class DisposableMongoClient : IMongoClient, IDisposable
    {
        private readonly IMongoClient wrapped;

        public DisposableMongoClient(IMongoClient wrapped)
        {
            this.wrapped = wrapped;
        }

        public ICluster Cluster => wrapped.Cluster;

        public MongoClientSettings Settings => wrapped.Settings;

        public void DropDatabase(string name, CancellationToken cancellationToken = default(CancellationToken))
        {
            wrapped.DropDatabase(name, cancellationToken);
        }

        public void DropDatabase(IClientSessionHandle session, string name, CancellationToken cancellationToken = default(CancellationToken))
        {
            wrapped.DropDatabase(session, name, cancellationToken);
        }

        public Task DropDatabaseAsync(string name, CancellationToken cancellationToken = default(CancellationToken))
        {
            return wrapped.DropDatabaseAsync(name, cancellationToken);
        }

        public Task DropDatabaseAsync(IClientSessionHandle session, string name, CancellationToken cancellationToken = default(CancellationToken))
        {
            return wrapped.DropDatabaseAsync(session, name, cancellationToken);
        }

        public IMongoDatabase GetDatabase(string name, MongoDatabaseSettings settings = null)
        {
            return wrapped.GetDatabase(name, settings);
        }

        public IEnumerable<string> ListDatabaseNames(
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return wrapped.ListDatabaseNames(cancellationToken);
        }

        public IEnumerable<string> ListDatabaseNames(
            IClientSessionHandle session,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return wrapped.ListDatabaseNames(session,cancellationToken);
        }

        public Task<IEnumerable<string>> ListDatabaseNamesAsync(
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return wrapped.ListDatabaseNamesAsync(cancellationToken);
        }

        public Task<IEnumerable<string>> ListDatabaseNamesAsync(
            IClientSessionHandle session,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return wrapped.ListDatabaseNamesAsync(session, cancellationToken);
        }

        public IAsyncCursor<BsonDocument> ListDatabases(
                ListDatabaseOptions options = null,
                CancellationToken cancellationToken = default(CancellationToken))
        {
            return wrapped.ListDatabases(options, cancellationToken);
        }

        public IAsyncCursor<BsonDocument> ListDatabases(
                IClientSessionHandle session, ListDatabaseOptions options = null,
                CancellationToken cancellationToken = default(CancellationToken))
        {
            return wrapped.ListDatabases(session, options, cancellationToken);
        }

        public Task<IAsyncCursor<BsonDocument>> ListDatabasesAsync(
                ListDatabaseOptions options = null,
                CancellationToken cancellationToken = default(CancellationToken))
        {
            return wrapped.ListDatabasesAsync(options, cancellationToken);
        }

        public Task<IAsyncCursor<BsonDocument>> ListDatabasesAsync(
                IClientSessionHandle session, ListDatabaseOptions options = null,
                CancellationToken cancellationToken = default(CancellationToken))
        {
            return wrapped.ListDatabasesAsync(session, options, cancellationToken);
        }

        public IClientSessionHandle StartSession(ClientSessionOptions options = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            return wrapped.StartSession(options, cancellationToken);
        }

        public Task<IClientSessionHandle> StartSessionAsync(ClientSessionOptions options = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            return wrapped.StartSessionAsync(options, cancellationToken);
        }

        public IMongoClient WithReadConcern(ReadConcern readConcern)
        {
            return wrapped.WithReadConcern(readConcern);
        }

        public IMongoClient WithReadPreference(ReadPreference readPreference)
        {
            return wrapped.WithReadPreference(readPreference);
        }

        public IMongoClient WithWriteConcern(WriteConcern writeConcern)
        {
            return wrapped.WithWriteConcern(writeConcern);
        }

        public void Dispose()
        {
            ClusterRegistry.Instance.UnregisterAndDisposeCluster(wrapped.Cluster);
        }
    }
}
