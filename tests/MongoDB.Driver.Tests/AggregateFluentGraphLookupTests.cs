/* Copyright 2016 MongoDB Inc.
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
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Bson.TestHelpers;
using MongoDB.Bson.TestHelpers.EqualityComparers;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.TestHelpers.XunitExtensions;
using Xunit;

namespace MongoDB.Driver.Tests
{
    public class AggregateFluentGraphTests
    {
        #region static
        // private static fields
        private static CollectionNamespace __collectionNamespace;
        private static IMongoDatabase __database;
        private static Lazy<bool> __ensureTestData;

        // static constructor
        static AggregateFluentGraphTests()
        {
            var databaseNamespace = DriverTestConfiguration.DatabaseNamespace;
            __database = DriverTestConfiguration.Client.GetDatabase(databaseNamespace.DatabaseName);
            __collectionNamespace = new CollectionNamespace(__database.DatabaseNamespace, "employees");
            __ensureTestData = new Lazy<bool>(CreateTestData);
        }

        // private static methods
        private static bool CreateTestData()
        {
            // test data is from: https://docs.mongodb.com/master/release-notes/3.4-reference/#pipe._S_graphLookup

            __database.DropCollection(__collectionNamespace.CollectionName);

            var collection = __database.GetCollection<BsonDocument>(__collectionNamespace.CollectionName);
            collection.InsertMany(new[]
                {
                    BsonDocument.Parse("{ _id : 1, name : 'Dev' }"),
                    BsonDocument.Parse("{ _id : 2, name : 'Eliot', reportsTo : 'Dev' }"),
                    BsonDocument.Parse("{ _id : 3, name : 'Ron', reportsTo : 'Eliot' }"),
                    BsonDocument.Parse("{ _id : 4, name : 'Andrew', reportsTo : 'Eliot' }"),
                    BsonDocument.Parse("{ _id : 5, name : 'Asya', reportsTo : 'Ron' }"),
                    BsonDocument.Parse("{ _id : 6, name : 'Dan', reportsTo : 'Andrew' }")
                });

            return true;
        }

        private static void EnsureTestData()
        {
            var _ = __ensureTestData.Value;
        }

        #endregion

        // public methods
        [Fact]
        public void GraphLookup_should_add_expected_stage()
        {
            var collection = __database.GetCollection<Employee>(__collectionNamespace.CollectionName);
            var subject = collection.Aggregate();
            var connectFromField = (FieldDefinition<Employee, string>)"ReportsTo";
            var connectToField = (FieldDefinition<Employee, string>)"Name";
            var startWith = (AggregateExpressionDefinition<Employee, string>)"$reportsTo";
            var @as = (FieldDefinition<EmployeeWithReportingHierarchy, IEnumerable<Employee>>)"ReportingHierarchy";

            var result = subject.GraphLookup(collection, connectFromField, connectToField, startWith, @as);

            var stage = result.Stages.Single();
            var renderedStage = stage.Render(collection.DocumentSerializer, BsonSerializer.SerializerRegistry);
            renderedStage.Document.Should().Be(
                @"{
                    $graphLookup : {
                        from : 'employees',
                        connectFromField : 'reportsTo',
                        connectToField : 'name',
                        startWith : '$reportsTo',
                        as : 'reportingHierarchy'
                    }
                }");
        }

        [SkippableFact]
        public void GraphLookup_should_return_expected_result()
        {
            RequireServer.Check().Supports(Feature.AggregateGraphLookupStage);
            EnsureTestData();
            var collection = __database.GetCollection<Employee>(__collectionNamespace.CollectionName);
            var subject = collection.Aggregate();
            var connectFromField = (FieldDefinition<Employee, string>)"ReportsTo";
            var connectToField = (FieldDefinition<Employee, string>)"Name";
            var startWith = (AggregateExpressionDefinition<Employee, string>)"$reportsTo";
            var @as = (FieldDefinition<EmployeeWithReportingHierarchy, IEnumerable<Employee>>)"ReportingHierarchy";

            var result = subject
                .GraphLookup(collection, connectFromField, connectToField, startWith, @as)
                .ToList();

            var comparer = new EmployeeWithReportingHierarchyEqualityComparer();
            var dev = new Employee { Id = 1, Name = "Dev", ReportsTo = null };
            var eliot = new Employee { Id = 2, Name = "Eliot", ReportsTo = "Dev" };
            var ron = new Employee { Id = 3, Name = "Ron", ReportsTo = "Eliot" };
            var andrew = new Employee { Id = 4, Name = "Andrew", ReportsTo = "Eliot" };
            var asya = new Employee { Id = 5, Name = "Asya", ReportsTo = "Ron" };
            var dan = new Employee { Id = 6, Name = "Dan", ReportsTo = "Andrew" };
            result.WithComparer(comparer).Should().Equal(
                new EmployeeWithReportingHierarchy { Id = 1, Name = "Dev", ReportsTo = null, ReportingHierarchy = new List<Employee>() },
                new EmployeeWithReportingHierarchy { Id = 2, Name = "Eliot", ReportsTo = "Dev", ReportingHierarchy = new List<Employee> { dev } },
                new EmployeeWithReportingHierarchy { Id = 3, Name = "Ron", ReportsTo = "Eliot", ReportingHierarchy = new List<Employee> { dev, eliot } },
                new EmployeeWithReportingHierarchy { Id = 4, Name = "Andrew", ReportsTo = "Eliot", ReportingHierarchy = new List<Employee> { dev, eliot } },
                new EmployeeWithReportingHierarchy { Id = 5, Name = "Asya", ReportsTo = "Ron", ReportingHierarchy = new List<Employee> { dev, eliot, ron } },
                new EmployeeWithReportingHierarchy { Id = 6, Name = "Dan", ReportsTo = "Andrew", ReportingHierarchy = new List<Employee> { dev, eliot, andrew } });
        }

        // nested types
        private class Employee
        {
            [BsonId]
            public int Id { get; set; }
            [BsonElement("name")]
            public string Name { get; set; }
            [BsonElement("reportsTo")]
            public string ReportsTo { get; set; }
        }

        private class EmployeeEqualityComparer : IEqualityComparer<Employee>
        {
            public bool Equals(Employee x, Employee y)
            {
                return
                    x.Id == y.Id &&
                    x.Name == y.Name &&
                    x.ReportsTo == y.ReportsTo;
            }

            public int GetHashCode(Employee obj)
            {
                throw new NotImplementedException();
            }
        }

        private class EmployeeWithReportingHierarchy
        {
            [BsonId]
            public int Id { get; set; }
            [BsonElement("name")]
            public string Name { get; set; }
            [BsonElement("reportsTo")]
            public string ReportsTo { get; set; }
            [BsonElement("reportingHierarchy")]
            public List<Employee> ReportingHierarchy { get; set; }
        }

        private class EmployeeWithReportingHierarchyEqualityComparer : IEqualityComparer<EmployeeWithReportingHierarchy>
        {
            private IEqualityComparer<List<Employee>> _reportingHierarchyComparer = new EnumerableSetEqualityComparer<Employee>(new EmployeeEqualityComparer());

            public bool Equals(EmployeeWithReportingHierarchy x, EmployeeWithReportingHierarchy y)
            {
                return
                    x.Id == y.Id &&
                    x.Name == y.Name &&
                    x.ReportsTo == y.ReportsTo &&
                    _reportingHierarchyComparer.Equals(x.ReportingHierarchy, y.ReportingHierarchy);
            }

            public int GetHashCode(EmployeeWithReportingHierarchy obj)
            {
                throw new NotImplementedException();
            }
        }
    }
}
