/* Copyright 2010-present MongoDB Inc.
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
using System.Reflection;
using FluentAssertions;
using MongoDB.Bson.TestHelpers;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Clusters.ServerSelectors;
using MongoDB.Driver.Core.Servers;
using MongoDB.Driver.Core.TestHelpers;
using Xunit;

namespace MongoDB.Driver.Tests
{
    public class MongoClientTests
    {
        [Fact]
        public void UsesSameMongoServerForIdenticalSettings()
        {
            var client1 = new MongoClient("mongodb://localhost");
#pragma warning disable 618
            var server1 = client1.GetServer();
#pragma warning restore

            var client2 = new MongoClient("mongodb://localhost");
#pragma warning disable 618
            var server2 = client2.GetServer();
#pragma warning restore

            Assert.Same(server1, server2);
        }

        [Fact]
        public void UsesSameMongoServerWhenReadPreferenceTagsAreTheSame()
        {
            var client1 = new MongoClient("mongodb://localhost/?readPreference=secondary;readPreferenceTags=dc:ny");
#pragma warning disable 618
            var server1 = client1.GetServer();
#pragma warning restore

            var client2 = new MongoClient("mongodb://localhost/?readPreference=secondary;readPreferenceTags=dc:ny");
#pragma warning disable 618
            var server2 = client2.GetServer();
#pragma warning restore

            Assert.Same(server1, server2);
        }

        [Theory]
        [InlineData("{ connectionMode : 'Automatic', clusterType : 'Standalone', servers : [ { state : 'Disconnected', type : 'Unknown' } ] }", null)]
        [InlineData("{ connectionMode : 'Automatic', clusterType : 'Standalone', servers : [ { state : 'Connected', type : 'Standalone' } ] }", false)]
        [InlineData("{ connectionMode : 'Automatic', clusterType : 'Standalone', servers : [ { state : 'Connected', type : 'Standalone', logicalSessionTimeoutMinutes : 30 } ] }", true)]
        [InlineData("{ connectionMode : 'Automatic', clusterType : 'ReplicaSet', servers : [ { state : 'Disconnected', type : 'Unknown' }, { state : 'Disconnected', type : 'Unknown' } ] }", null)]
        [InlineData("{ connectionMode : 'Automatic', clusterType : 'ReplicaSet', servers : [ { state : 'Disconnected', type : 'Unknown' }, { state : 'Connected', type : 'ReplicaSetPrimary' } ] }", false)]
        [InlineData("{ connectionMode : 'Automatic', clusterType : 'ReplicaSet', servers : [ { state : 'Disconnected', type : 'Unknown' }, { state : 'Connected', type : 'ReplicaSetSecondary' } ] }", false)]
        [InlineData("{ connectionMode : 'Automatic', clusterType : 'ReplicaSet', servers : [ { state : 'Disconnected', type : 'Unknown' }, { state : 'Connected', type : 'ReplicaSetArbiter' } ] }", null)]
        [InlineData("{ connectionMode : 'Automatic', clusterType : 'ReplicaSet', servers : [ { state : 'Disconnected', type : 'Unknown' }, { state : 'Connected', type : 'ReplicaSetPrimary', logicalSessionTimeoutMinutes : 30 } ] }", true)]
        [InlineData("{ connectionMode : 'Automatic', clusterType : 'ReplicaSet', servers : [ { state : 'Disconnected', type : 'Unknown' }, { state : 'Connected', type : 'ReplicaSetSecondary', logicalSessionTimeoutMinutes : 30 } ] }", true)]
        [InlineData("{ connectionMode : 'Automatic', clusterType : 'ReplicaSet', servers : [ { state : 'Disconnected', type : 'Unknown' }, { state : 'Connected', type : 'ReplicaSetArbiter', logicalSessionTimeoutMinutes : 30 } ] }", null)]
        [InlineData("{ connectionMode : 'Automatic', clusterType : 'Sharded', servers : [ { state : 'Disconnected', type : 'Unknown' }, { state : 'Disconnected', type : 'Unknown' } ] }", null)]
        [InlineData("{ connectionMode : 'Automatic', clusterType : 'Sharded', servers : [ { state : 'Disconnected', type : 'Unknown' }, { state : 'Connected', type : 'ShardRouter' } ] }", false)]
        [InlineData("{ connectionMode : 'Automatic', clusterType : 'Sharded', servers : [ { state : 'Disconnected', type : 'Unknown' }, { state : 'Connected', type : 'ShardRouter', logicalSessionTimeoutMinutes : 30 } ] }", true)]
        [InlineData("{ connectionMode : 'Direct', clusterType : 'ReplicaSet', servers : [ { state : 'Disconnected', type : 'Unknown' } ] }", null)]
        [InlineData("{ connectionMode : 'Direct', clusterType : 'ReplicaSet', servers : [ { state : 'Connected', type : 'ReplicaSetOther' } ] }", false)]
        [InlineData("{ connectionMode : 'Direct', clusterType : 'ReplicaSet', servers : [ { state : 'Connected', type : 'ReplicaSetOther', logicalSessionTimeoutMinutes : 30 } ] }", false)]
        [InlineData("{ connectionMode : 'Direct', clusterType : 'ReplicaSet', servers : [ { state : 'Connected', type : 'ReplicaSetPrimary' } ] }", false)]
        [InlineData("{ connectionMode : 'Direct', clusterType : 'ReplicaSet', servers : [ { state : 'Connected', type : 'ReplicaSetPrimary', logicalSessionTimeoutMinutes : 30 } ] }", true)]
        public void AreSessionsSupported_should_return_expected_result(string clusterDescriptionJson, bool? expectedResult)
        {
            var subject = new MongoClient("mongodb://localhost");
            var clusterDescription = ClusterDescriptionParser.Parse(clusterDescriptionJson);

            var result = subject.AreSessionsSupported(clusterDescription);

            result.Should().Be(expectedResult);
        }
    }

    public class AreSessionsSupportedServerSelectorTests
    {
        [Theory]
        [InlineData("{ clusterType : 'Standalone', servers : [ { state : 'Disconnected', type : 'Unknown' } ]}")]
        public void SelectServers_should_set_ClusterDescription(string clusterDescriptionJson)
        {
            var subject = CreateSubject();
            var cluster = ClusterDescriptionParser.Parse(clusterDescriptionJson);
            var connectedServers = cluster.Servers.Where(s => s.State == ServerState.Connected);

            var result = subject.SelectServers(cluster, connectedServers);

            AreSessionsSupportedServerSelectorReflector.ClusterDescription(subject).Should().BeSameAs(cluster);
        }

        [Theory]
        [InlineData("{ connectionMode : 'Direct', clusterType : 'ReplicaSet', servers : [ { state : 'Connected', type : 'ReplicaSetArbiter' } ]}")]
        public void SelectServers_should_return_all_servers_when_connection_mode_is_direct(string clusterDescriptionJson)
        {
            var subject = CreateSubject();
            var cluster = ClusterDescriptionParser.Parse(clusterDescriptionJson);
            var connectedServers = cluster.Servers.Where(s => s.State == ServerState.Connected).ToList();

            var result = subject.SelectServers(cluster, connectedServers);

            result.Should().BeSameAs(connectedServers);
        }

        [Theory]
        [InlineData("{ clusterType : 'ReplicaSet', servers : [ { state : 'Connected', type : 'ReplicaSetArbiter' }, { state : 'Connected', type : 'ReplicaSetPrimary' } ]}")]
        public void SelectServers_should_return_data_bearing_servers_when_connection_mode_is__not_direct(string clusterDescriptionJson)
        {
            var subject = CreateSubject();
            var cluster = ClusterDescriptionParser.Parse(clusterDescriptionJson);
            var connectedServers = cluster.Servers.Where(s => s.State == ServerState.Connected).ToList();
            var dataBearingServers = connectedServers.Skip(1).Take(1);

            var result = subject.SelectServers(cluster, connectedServers);

            result.Should().Equal(dataBearingServers);
        }

        // private methods
        private IServerSelector CreateSubject()
        {
            return AreSessionsSupportedServerSelectorReflector.CreateInstance();
        }
    }

    public static class MongoClientReflector
    {
        public static bool? AreSessionsSupported(this MongoClient obj, ClusterDescription clusterDescription) => (bool?)Reflector.Invoke(obj, nameof(AreSessionsSupported), clusterDescription);
    }

    public static class AreSessionsSupportedServerSelectorReflector
    {
        public static IServerSelector CreateInstance()
        {
            var type = typeof(MongoClient).GetTypeInfo().Assembly.GetType("MongoDB.Driver.MongoClient+AreSessionsSupportedServerSelector");
            return (IServerSelector)Activator.CreateInstance(type);
        }

        public static ClusterDescription ClusterDescription(IServerSelector obj) => (ClusterDescription)Reflector.GetFieldValue(obj, nameof(ClusterDescription), BindingFlags.Public | BindingFlags.Instance);
    }
}
