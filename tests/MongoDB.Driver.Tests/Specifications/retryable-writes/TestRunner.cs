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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using MongoDB.Bson;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.TestHelpers.XunitExtensions;
using Xunit;
using Xunit.Abstractions;

namespace MongoDB.Driver.Tests.Specifications.retryable_writes
{
    public class TestRunner
    {
        private static Dictionary<string, Func<ICrudOperationTest>> _tests;
        private static bool __oneTimeSetupHasRun = false;
        private static object __oneTimeSetupLock = new object();

        public TestRunner()
        {
            lock (__oneTimeSetupLock)
            {
                __oneTimeSetupHasRun = __oneTimeSetupHasRun || OneTimeSetup();
            }
        }

        public bool OneTimeSetup()
        {
            _tests = new Dictionary<string, Func<ICrudOperationTest>>
            {
                { "findOneAndDelete", () => new FindOneAndDeleteTest() },
                { "findOneAndReplace", () => new FindOneAndReplaceTest() },
                { "findOneAndUpdate", () => new FindOneAndUpdateTest() },
            };

            return true;
        }

        [SkippableTheory]
        [ClassData(typeof(TestCaseFactory))]
        public void RunTestDefinition(TestCase testCase)
        {
            RequireServer.Check().ClusterType(ClusterType.ReplicaSet);

            //BsonValue minServerVersion;
            //if (definition.TryGetValue("minServerVersion", out minServerVersion))
            //{
            //    RequireServer.Check().VersionGreaterThanOrEqualTo(minServerVersion.AsString);
            //}

            //BsonValue maxServerVersion;
            //if (definition.TryGetValue("maxServerVersion", out maxServerVersion))
            //{
            //    RequireServer.Check().VersionLessThanOrEqualTo(maxServerVersion.AsString);
            //}

            var connectionString = CoreTestConfiguration.ConnectionString.ToString();
            var clientSettings = MongoClientSettings.FromUrl(new MongoUrl(connectionString));
            clientSettings.RetryWrites = true;

            var client = new MongoClient(clientSettings);
            var database = client.GetDatabase(DriverTestConfiguration.DatabaseNamespace.DatabaseName);
            var collection = database.GetCollection<BsonDocument>(DriverTestConfiguration.CollectionNamespace.CollectionName);

            database.DropCollection(collection.CollectionNamespace.CollectionName);
            collection.InsertMany(testCase.Definition["data"].AsBsonArray.Cast<BsonDocument>());

            BsonValue failPointDoc = null;
            testCase.Test.TryGetValue("failPoint", out failPointDoc);

            using (WithFailPoint((BsonDocument)failPointDoc))
            {
                ExecuteOperation(database, collection, (BsonDocument)testCase.Test["operation"], (BsonDocument)testCase.Test["outcome"], testCase.Async);
            }
        }

        private void ExecuteOperation(IMongoDatabase database, IMongoCollection<BsonDocument> collection, BsonDocument operation, BsonDocument outcome, bool async)
        {
            var name = (string)operation["name"];
            Func<ICrudOperationTest> factory;
            if (!_tests.TryGetValue(name, out factory))
            {
                throw new NotImplementedException("The operation " + name + " has not been implemented.");
            }

            var arguments = (BsonDocument)operation.GetValue("arguments", new BsonDocument());
            var test = factory();
            string reason;
            if (!test.CanExecute(DriverTestConfiguration.Client.Cluster.Description, arguments, out reason))
            {
                throw new SkipTestException(reason);
            }

            test.Execute(DriverTestConfiguration.Client.Cluster.Description, database, collection, arguments, outcome, async);
        }

        public class TestCase : IXunitSerializable
        {
            public BsonDocument Definition;
            public BsonDocument Test;
            public bool Async;
            public string Name;

            public TestCase()
            {
            }

            public TestCase(string name, BsonDocument definition, BsonDocument test, bool async)
            {
                Name = name;
                Definition = definition;
                Test = test;
                Async = async;
            }

            public void Deserialize(IXunitSerializationInfo info)
            {
                Name = info.GetValue<string>(nameof(Name));
                Definition = BsonDocument.Parse(info.GetValue<string>(nameof(Definition)));
                Test = BsonDocument.Parse(info.GetValue<string>(nameof(Test)));
                Async = info.GetValue<bool>(nameof(Async));
            }

            public void Serialize(IXunitSerializationInfo info)
            {
                info.AddValue(nameof(Name), Name);
                info.AddValue(nameof(Definition), Definition.ToString());
                info.AddValue(nameof(Test), Test.ToString());
                info.AddValue(nameof(Async), Async);
            }

            public override string ToString()
            {
                if (Async)
                {
                    return Name + "(Async)";
                }

                return Name;
            }
        }

        private class TestCaseFactory : IEnumerable<object[]>
        {
            public IEnumerator<object[]> GetEnumerator()
            {
                const string prefix = "MongoDB.Driver.Tests.Specifications.retryable_writes.tests.";
                var definitions = typeof(TestCaseFactory).GetTypeInfo().Assembly
                    .GetManifestResourceNames()
                    .Where(path => path.StartsWith(prefix) && path.EndsWith(".json"))
                    .Select(path => ReadDefinition(path));

                var testCases = new List<object[]>();
                foreach (var definition in definitions)
                {
                    foreach (BsonDocument test in definition["tests"].AsBsonArray)
                    {
                        foreach (var async in new[] { false, true })
                        {
                            //var testCase = new TestCaseData(definition, test, async);
                            //testCase.SetCategory("Specifications");
                            //testCase.SetCategory("crud");
                            //testCase.SetName($"{test["description"]}({async})");
                            var name = test["description"].ToString();
                            var testCase = new object[] { new TestCase(name, definition, test, async) };
                            testCases.Add(testCase);
                        }
                    }
                }

                return testCases.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private static BsonDocument ReadDefinition(string path)
            {
                using (var definitionStream = typeof(TestCaseFactory).GetTypeInfo().Assembly.GetManifestResourceStream(path))
                using (var definitionStringReader = new StreamReader(definitionStream))
                {
                    var definitionString = definitionStringReader.ReadToEnd();
                    var definition = BsonDocument.Parse(definitionString);
                    definition.InsertAt(0, new BsonElement("path", path));
                    return definition;
                }
            }
        }

        private IDisposable WithFailPoint(BsonDocument failPoint)
        {
            if(failPoint == null)
            {
                return new EmptyDisposer();
            }

            var command = new BsonDocument("configureFailPoint", "onPrimaryTransactionalWrite").Merge(failPoint, true);

            DriverTestConfiguration.Client.GetDatabase("admin").RunCommand<BsonDocument>(command);

            return new ActionDisposer(() => {
                DriverTestConfiguration.Client.GetDatabase("admin").RunCommand<BsonDocument>(new BsonDocument
                {
                    { "configureFailPoint", "onPrimaryTransactionalWrite" },
                    { "mode", "off" }
                });
            });
        }

        private class EmptyDisposer : IDisposable
        {
            public void Dispose()
            {
            }
        }

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
