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

using System.Reflection;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core.Clusters;
using Moq;
using Xunit;

namespace MongoDB.Driver.Core.Bindings
{
    public class CoreSessionTests
    {
        [Fact]
        public void constructor_should_initialize_instance()
        {
            var cluster = Mock.Of<ICluster>();
            var serverSession = Mock.Of<ICoreServerSession>();
            var options = new CoreSessionOptions();

            var result = new CoreSession(cluster, serverSession, options);

            result.Cluster.Should().BeSameAs(cluster);
            result.Options.Should().BeSameAs(options);
            result.ServerSession.Should().BeSameAs(serverSession);
            result._disposed().Should().BeFalse();
        }

        [Fact]
        public void ClusterTime_should_return_expected_result()
        {
            var subject = CreateSubject();
            var clusterTime = new BsonDocument();
            subject.AdvanceClusterTime(clusterTime);

            var result = subject.ClusterTime;

            result.Should().BeSameAs(clusterTime);
        }

        [Fact]
        public void Id_should_should_call_serverSession()
        {
            var subject = CreateSubject();
            var id = new BsonDocument();
            Mock.Get(subject.ServerSession).SetupGet(m => m.Id).Returns(id);

            var result = subject.Id;

            result.Should().Be(id);
        }

        [Theory]
        [ParameterAttributeData]
        public void IsCausallyConsistent_should_return_expected_result(
            [Values(false, true)] bool value)
        {
            var options = new CoreSessionOptions(isCausallyConsistent: value);
            var subject = CreateSubject(options: options);

            var result = subject.IsCausallyConsistent;

            result.Should().Be(value);
        }

        [Theory]
        [ParameterAttributeData]
        public void IsImplicit_should_return_expected_result(
            [Values(false, true)] bool value)
        {
            var options = new CoreSessionOptions(isImplicit: value);
            var subject = CreateSubject(options: options);

            var result = subject.IsImplicit;

            result.Should().Be(value);
        }

        [Fact]
        public void OperationTime_should_return_expected_result()
        {
            var subject = CreateSubject();
            var operationTime = new BsonTimestamp(0);
            subject.AdvanceOperationTime(operationTime);

            var result = subject.OperationTime;

            result.Should().BeSameAs(operationTime);
        }

        [Fact]
        public void ServerSession_should_return_expected_result()
        {
            var serverSession = Mock.Of<ICoreServerSession>();
            var subject = CreateSubject(serverSession: serverSession);

            var result = subject.ServerSession;

            result.Should().BeSameAs(serverSession);
        }

        [Fact]
        public void AdvanceClusterTime_should_have_expected_result()
        {
            var subject = CreateSubject();
            var newClusterTime = new BsonDocument();

            subject.AdvanceClusterTime(newClusterTime);

            subject.ClusterTime.Should().BeSameAs(newClusterTime);
        }

        [Fact]
        public void AdvanceOperationTime_should_have_expected_result()
        {
            var subject = CreateSubject();
            var newOperationTime = new BsonTimestamp(0);

            subject.AdvanceOperationTime(newOperationTime);

            subject.OperationTime.Should().BeSameAs(newOperationTime);
        }

        [Theory]
        [ParameterAttributeData]
        public void Dispose_should_have_expected_result(
            [Values(1, 2)] int timesCalled)
        {
            var subject = CreateSubject();

            for (var i = 0; i < timesCalled; i++)
            {
                subject.Dispose();
            }

            subject._disposed().Should().BeTrue();
            Mock.Get(subject.ServerSession).Verify(m => m.Dispose(), Times.Once);
        }

        [Fact]
        public void WasUsed_should_call_serverSession()
        {
            var subject = CreateSubject();

            subject.WasUsed();

            Mock.Get(subject.ServerSession).Verify(m => m.WasUsed(), Times.Once);
        }

        // private methods
        private CoreSession CreateSubject(
            ICluster cluster = null,
            ICoreServerSession serverSession = null,
            CoreSessionOptions options = null)
        {
            cluster = cluster ?? Mock.Of<ICluster>();
            serverSession = serverSession ?? Mock.Of<ICoreServerSession>();
            options = options ?? new CoreSessionOptions();
            return new CoreSession(cluster, serverSession, options);
        }
    }

    public static class CoreSessionReflector
    {
        public static bool _disposed(this CoreSession obj)
        {
            var fieldInfo = typeof(CoreSession).GetField("_disposed", BindingFlags.NonPublic | BindingFlags.Instance);
            return (bool)fieldInfo.GetValue(obj);
        }
    }
}
