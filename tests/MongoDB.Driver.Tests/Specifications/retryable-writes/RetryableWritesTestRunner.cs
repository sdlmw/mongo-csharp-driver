/* Copyright 2017-present MongoDB Inc.
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
using System.Linq;
using System.Threading;
using MongoDB.Bson;
using MongoDB.Bson.TestHelpers.JsonDrivenTests;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.TestHelpers.XunitExtensions;
using MongoDB.Driver.Tests.JsonDrivenTests;
using Xunit;

namespace MongoDB.Driver.Tests.Specifications.retryable_writes
{
    public class RetryableWritesTestRunner : JsonDrivenClientTestRunner
    {
        // private fields
        private readonly string _databaseName = DriverTestConfiguration.DatabaseNamespace.DatabaseName;
        private readonly string _collectionName = DriverTestConfiguration.CollectionNamespace.CollectionName;

        // public methods
        [SkippableTheory]
        [ClassData(typeof(RetryableWritesTestCaseFactory))]
        public void Run(JsonDrivenTestCase testCase)
        {
            RequireServer.Check().ClusterType(ClusterType.ReplicaSet);
            Run(testCase.Shared, testCase.Test);
        }

        // private methods
        private void Run(BsonDocument shared, BsonDocument test)
        {
            JsonDrivenHelper.EnsureAllFieldsAreValid(shared, "_path", "data", "minServerVersion", "tests");
            JsonDrivenHelper.EnsureAllFieldsAreValid(test, "description", "clientOptions", "failPoint", "operation", "outcome");

            RequireServerVersion(shared);
            InitializeCollection(shared);

            var client = CreateClient(test);
            using (ConfigureFailPoint(client, test))
            {
                ExecuteOperation(client, test);
            }
        }

        private void InitializeCollection(BsonDocument shared)
        {
            var client = DriverTestConfiguration.Client;
            var database = client.GetDatabase(_databaseName);
            var collection = database.GetCollection<BsonDocument>(_collectionName);

            DropCollection(database, _collectionName);
            InsertData(collection, shared);
        }

        private IMongoClient CreateClient(BsonDocument test)
        {
            var settings = ParseClientOptions(test);
            return new MongoClient(settings);
        }

        private MongoClientSettings ParseClientOptions(BsonDocument test)
        {
            var settings = new MongoClientSettings();

            if (test.Contains("clientOptions"))
            {
                foreach (var element in test["clientOptions"].AsBsonDocument)
                {
                    var name = element.Name;
                    var value = element.Value;

                    switch (name)
                    {
                        case "retryWrites":
                            settings.RetryWrites = value.ToBoolean();
                            break;

                        default:
                            throw new FormatException($"Invalid clientOption: {name}.");
                    }
                }
            }

            return settings;
        }

        private void ExecuteOperation(IMongoClient client, BsonDocument test)
        {
            var database = client.GetDatabase(_databaseName);
            var collection = database.GetCollection<BsonDocument>(_collectionName);
            var factory = new JsonDrivenClientTestFactory(client, database, collection, null);

            var operation = test["operation"].AsBsonDocument;
            var operationName = operation["name"].AsString;

            var outcome = test["outcome"].AsBsonDocument;
            operation["result"] = outcome["result"];
            outcome.Remove("result");

            var executableTest = factory.CreateTest(operationName);

            executableTest.Arrange(operation);
            executableTest.Act(CancellationToken.None);
            executableTest.Assert();

            AssertOutcome(collection, outcome);
        }

        private void AssertOutcome(IMongoCollection<BsonDocument> collection, BsonDocument outcome)
        {
            JsonDrivenHelper.EnsureAllFieldsAreValid(outcome, "collection");
            var expectedCollection = outcome["collection"].AsBsonDocument;

            JsonDrivenHelper.EnsureAllFieldsAreValid(expectedCollection, "data");
            var expectedDocuments = expectedCollection["data"].AsBsonArray.Cast<BsonDocument>().ToList();

            AssertCollectionContents(collection, expectedDocuments);
        }

        // nested types
        public class RetryableWritesTestCaseFactory : JsonDrivenTestCaseFactory
        {
            // protected properties
            protected override string PathPrefix
            {
                get
                {
#if NET45
                    return "MongoDB.Driver.Tests.Specifications.retryable_writes.tests.";
#else
                    return "MongoDB.Driver.Tests.Dotnet.Specifications.retryable_writes.tests.";
#endif
                }
            }

            protected override bool ShouldReadJsonDocument(string path)
            {
                return base.ShouldReadJsonDocument(path) && path.EndsWith("bulkWrite.json");
            }
        }
    }
}
