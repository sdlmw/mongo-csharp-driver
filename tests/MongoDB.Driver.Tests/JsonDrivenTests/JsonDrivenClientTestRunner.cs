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
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver.Core.TestHelpers.XunitExtensions;

namespace MongoDB.Driver.Tests.JsonDrivenTests
{
    public abstract class JsonDrivenClientTestRunner
    {
        // protected methods
        protected void AssertCollectionContents(IMongoCollection<BsonDocument> collection, List<BsonDocument> expectedDocuments)
        {
            var actualDocuments = collection.Find("{}").ToList();
            actualDocuments.Should().BeEquivalentTo(expectedDocuments);
        }

        protected IDisposable ConfigureFailPoint(IMongoClient client, BsonDocument test)
        {
            if (test.Contains("failPoint"))
            {
                var failPoint = test["failPoint"].AsBsonDocument;

                var adminDatabase = client.GetDatabase("admin");
                adminDatabase.RunCommand<BsonDocument>(failPoint);

                var failPointName = failPoint["configureFailPoint"].AsString;
                var disableFailPointCommand = new BsonDocument
                {
                    { "configureFailPoint", failPointName },
                    { "mode", "off" }
                };
                return new ActionDisposer(() => adminDatabase.RunCommand<BsonDocument>(disableFailPointCommand));
            }
            else
            {
                return null;
            }
        }

        protected void CreateCollection(IMongoDatabase database, string collectionName)
        {
            database.CreateCollection(collectionName);
        }

        protected void DropCollection(IMongoDatabase database, string collectionName)
        {
            database.DropCollection(collectionName);
        }

        protected void InsertData(IMongoCollection<BsonDocument> collection, BsonDocument shared)
        {
            if (shared.Contains("data"))
            {
                var documents = shared["data"].AsBsonArray.Cast<BsonDocument>().ToList();
                if (documents.Count > 0)
                {
                    collection.InsertMany(documents);
                }
            }
        }

        protected void RequireServerVersion(BsonDocument shared)
        {
            BsonValue minServerVersion;
            if (shared.TryGetValue("minServerVersion", out minServerVersion))
            {
                RequireServer.Check().VersionGreaterThanOrEqualTo(minServerVersion.AsString);
            }
        }

        // nested types
        private class ActionDisposer : IDisposable
        {
            private readonly Action _action;

            public ActionDisposer(Action action)
            {
                _action = action;
            }

            public void Dispose()
            {
                _action();
            }
        }
    }
}
