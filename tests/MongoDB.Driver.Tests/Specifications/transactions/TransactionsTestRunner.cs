﻿/* Copyright 2018-present MongoDB Inc.
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
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.TestHelpers.JsonDrivenTests;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Events;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.TestHelpers.JsonDrivenTests;
using MongoDB.Driver.Core.TestHelpers.XunitExtensions;
using MongoDB.Driver.TestHelpers;
using MongoDB.Driver.Tests.JsonDrivenTests;
using Xunit;

namespace MongoDB.Driver.Tests.Specifications.transactions
{
    public sealed class TransactionsTestRunner : JsonDrivenClientTestRunner
    {
        #region static
        private static readonly HashSet<string> __commandsToNotCapture = new HashSet<string>
        {
            "isMaster",
            "buildInfo",
            "getLastError",
            "authenticate",
            "saslStart",
            "saslContinue",
            "getnonce"
        };
        #endregion

        // constants
        private const string __databaseName = "transaction-tests";
        private const string __collectionName = "test";

        // public methods
        [SkippableTheory]
        [ClassData(typeof(TransactionsTestCaseFactory))]
        public void Run(JsonDrivenTestCase testCase)
        {
            RequireServer.Check().Supports(Feature.Transactions).ClusterType(ClusterType.ReplicaSet);
            Run(testCase.Shared, testCase.Test);
        }

        // private methods
        private void Run(BsonDocument shared, BsonDocument test)
        {
            if (test.Contains("skipReason"))
            {
                throw new SkipTestException(test["skipReason"].AsString);
            }
            //if (test["description"].AsString != "startTransaction options override defaults")
            //{
            //    throw new SkipTestException("testing");
            //}

            JsonDrivenHelper.EnsureAllFieldsAreValid(shared, "_path", "data", "tests");
            JsonDrivenHelper.EnsureAllFieldsAreValid(test, "description", "clientOptions", "sessionOptions", "operations", "expectations", "outcome");

            InitializeCollection(shared);

            var eventCapturer = new EventCapturer()
                .Capture<CommandStartedEvent>(e => !__commandsToNotCapture.Contains(e.CommandName));

            Dictionary<string, BsonValue> sessionIdMap;

            using (var client = CreateDisposableClient(test, eventCapturer))
            using (var session0 = StartSession(client, test, "session0"))
            using (var session1 = StartSession(client, test, "session1"))
            {
                var sessionMap = new Dictionary<string, IClientSessionHandle>
                {
                    { "session0", session0 },
                    { "session1", session1 }
                };
                sessionIdMap = new Dictionary<string, BsonValue>
                {
                    { "session0", session0.ServerSession.Id },
                    { "session1", session1.ServerSession.Id }
                };

                ExecuteOperations(client, sessionMap, test);
            }

            AssertEvents(eventCapturer, test, sessionIdMap);
            AssertOutcome(test);
        }

        private void InitializeCollection(BsonDocument shared)
        {
            var client = DriverTestConfiguration.Client;
            var database = client.GetDatabase(__databaseName).WithWriteConcern(WriteConcern.WMajority);
            var collection = database.GetCollection<BsonDocument>(__collectionName);

            DropCollection(database, __collectionName);
            CreateCollection(database, __collectionName);
            InsertData(collection, shared);
        }

        private DisposableMongoClient CreateDisposableClient(BsonDocument test, EventCapturer eventCapturer)
        {
            return DriverTestConfiguration.CreateDisposableClient((MongoClientSettings settings) =>
            {
                ConfigureClientSettings(settings, test);
                settings.ClusterConfigurator = c => c.Subscribe(eventCapturer);
            });
        }

        private void ConfigureClientSettings(MongoClientSettings settings, BsonDocument test)
        {
            if (test.Contains("clientOptions"))
            {
                foreach (var option in test["clientOptions"].AsBsonDocument)
                {
                    switch (option.Name)
                    {
                        case "readConcernLevel":
                            var level = (ReadConcernLevel)Enum.Parse(typeof(ReadConcernLevel), option.Value.AsString, ignoreCase: true);
                            settings.ReadConcern = new ReadConcern(level);
                            break;

                        case "retryWrites":
                            settings.RetryWrites = option.Value.ToBoolean();
                            break;

                        case "w":
                            if (option.Value.IsString)
                            {
                                settings.WriteConcern = new WriteConcern(option.Value.AsString);
                            }
                            else
                            {
                                settings.WriteConcern = new WriteConcern(option.Value.ToInt32());
                            }
                            break;

                        default:
                            throw new FormatException($"Unexpected client option: \"{option.Name}\".");
                    }
                }
            }
        }

        private IClientSessionHandle StartSession(IMongoClient client, BsonDocument test, string sessionKey)
        {
            var options = CreateSessionOptions(test, sessionKey);
            return client.StartSession(options);
        }

        private ClientSessionOptions CreateSessionOptions(BsonDocument test, string sessionKey)
        {
            var options = new ClientSessionOptions();
            if (test.Contains("sessionOptions"))
            {
                var sessionOptions = test["sessionOptions"].AsBsonDocument;
                if (sessionOptions.Contains(sessionKey))
                {
                    foreach (var option in sessionOptions[sessionKey].AsBsonDocument)
                    {
                        switch (option.Name)
                        {
                            case "autoStartTransaction":
                                options.AutoStartTransaction = option.Value.ToBoolean();
                                break;

                            case "causalConsistency":
                                options.CausalConsistency = option.Value.ToBoolean();
                                break;

                            case "defaultTransactionOptions":
                                options.DefaultTransactionOptions = ParseTransactionOptions(option.Value.AsBsonDocument);
                                break;

                            default:
                                throw new FormatException($"Unexpected session option: \"{option.Name}\".");
                        }
                    }
                }
            }
            return options;
        }

        private void ExecuteOperations(IMongoClient client, Dictionary<string, IClientSessionHandle> sessionMap, BsonDocument test)
        {
            var factory = CreateTestFactory(client, sessionMap);
            foreach (var operation in test["operations"].AsBsonArray.Cast<BsonDocument>())
            {
                var jsonDrivenTest = factory.CreateTest(operation["name"].AsString);
                jsonDrivenTest.Arrange(operation);
                jsonDrivenTest.Act(CancellationToken.None);
                jsonDrivenTest.Assert();
            }
        }

        private JsonDrivenClientTestFactory CreateTestFactory(IMongoClient client, Dictionary<string, IClientSessionHandle> sessionMap)
        {
            var database = client.GetDatabase(__databaseName);
            var collection = database.GetCollection<BsonDocument>(__collectionName);
            return new JsonDrivenClientTestFactory(client, database, collection, sessionMap);
        }

        private void AssertEvents(EventCapturer actualEvents, BsonDocument test, Dictionary<string, BsonValue> sessionIdMap)
        {
            if (test.Contains("expectations"))
            {
                foreach (var expectedEvent in test["expectations"].AsBsonArray.Cast<BsonDocument>())
                {
                    RecursiveFieldSetter.SetAll(expectedEvent, "lsid", value => sessionIdMap[value.AsString]);
                    var actualEvent = actualEvents.Next();
                    AssertEvent(actualEvent, expectedEvent);
                }
            }
        }

        private void AssertEvent(object actualEvent, BsonDocument expectedEvent)
        {
            if (expectedEvent.ElementCount != 1)
            {
                throw new FormatException("Expected event must be a document with a single element with a name the specifies the type of the event.");
            }

            var eventType = expectedEvent.GetElement(0).Name;
            var eventAsserter = EventAsserterFactory.CreateAsserter(eventType);
            eventAsserter.AssertAspects(actualEvent, expectedEvent[0].AsBsonDocument);
        }

        private void AssertOutcome(BsonDocument test)
        {
            if (test.Contains("outcome"))
            {
                foreach (var aspect in test["outcome"].AsBsonDocument)
                {
                    switch (aspect.Name)
                    {
                        case "collection":
                            VerifyCollectionOutcome(aspect.Value.AsBsonDocument);
                            break;

                        default:
                            throw new FormatException($"Unexpected outcome aspect: {aspect.Name}.");
                    }
                }
            }
        }

        public TransactionOptions ParseTransactionOptions(BsonDocument document)
        {
            ReadConcern readConcern = null;
            WriteConcern writeConcern = null;

            foreach (var element in document)
            {
                switch (element.Name)
                {
                    case "readConcern":
                        readConcern = ReadConcern.FromBsonDocument(element.Value.AsBsonDocument);
                        break;

                    case "writeConcern":
                        writeConcern = WriteConcern.FromBsonDocument(element.Value.AsBsonDocument);
                        break;

                    default:
                        throw new ArgumentException($"Invalid field: {element.Name}.");
                }
            }

            return new TransactionOptions(readConcern, writeConcern);
        }

        private void VerifyCollectionOutcome(BsonDocument outcome)
        {
            foreach (var aspect in outcome)
            {
                switch (aspect.Name)
                {
                    case "data":
                        VerifyCollectionData(aspect.Value.AsBsonArray.Cast<BsonDocument>());
                        break;

                    default:
                        throw new FormatException($"Unexpected collection outcome aspect: {aspect.Name}.");
                }
            }
        }

        private void VerifyCollectionData(IEnumerable<BsonDocument> expectedDocuments)
        {
            var database = DriverTestConfiguration.Client.GetDatabase(__databaseName);
            var collection = database.GetCollection<BsonDocument>(__collectionName);
            var actualDocuments = collection.Find("{}").ToList();
            actualDocuments.Should().BeEquivalentTo(expectedDocuments);
        }

        // nested types
        public class TransactionsTestCaseFactory : JsonDrivenTestCaseFactory
        {
            // protected properties
            protected override string PathPrefix
            {
                get
                {
#if NET45
                    return "MongoDB.Driver.Tests.Specifications.transactions.tests.";
#else
                    return "MongoDB.Driver.Tests.Dotnet.Specifications.transactions.tests.";
#endif
                }
            }
        }
    }
}
