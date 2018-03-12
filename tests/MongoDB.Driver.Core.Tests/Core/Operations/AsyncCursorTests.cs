﻿/* Copyright 2015-present MongoDB Inc.
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
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Servers;
using MongoDB.Driver.Core.TestHelpers;
using MongoDB.Driver.Core.TestHelpers.XunitExtensions;
using MongoDB.Driver.Core.WireProtocol;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;
using Moq;
using Xunit;

namespace MongoDB.Driver.Core.Operations
{
    // public methods
    public class AsyncCursorTests
    {
        // public methods
        [Fact]
        public void constructor_should_dispose_channel_source_when_cursor_id_is_zero()
        {
            var mockChannelSource = new Mock<IChannelSource>();
            var subject = CreateSubject(cursorId: 0, channelSource: Optional.Create(mockChannelSource.Object));

            mockChannelSource.Verify(s => s.Dispose(), Times.Once);
        }

        [Fact]
        public void constructor_should_initialize_instance()
        {
            var channelSource = new Mock<IChannelSource>().Object;
            var databaseNamespace = new DatabaseNamespace("test");
            var collectionNamespace = new CollectionNamespace(databaseNamespace, "test");
            var query = new BsonDocument("x", 1);
            var firstBatch = new BsonDocument[] { new BsonDocument("y", 2) };
            var cursorId = 1L;
            var batchSize = 2;
            var limit = 3;
            var serializer = BsonDocumentSerializer.Instance;
            var messageEncoderSettings = new MessageEncoderSettings();
            var maxTime = TimeSpan.FromSeconds(1);

            var result = new AsyncCursor<BsonDocument>(
                channelSource,
                collectionNamespace,
                query,
                firstBatch,
                cursorId,
                batchSize,
                limit,
                serializer,
                messageEncoderSettings,
                maxTime);

            result._batchSize().Should().Be(batchSize);
            result._channelSource().Should().Be(channelSource);
            result._collectionNamespace().Should().Be(collectionNamespace);
            result._count().Should().Be(firstBatch.Length);
            result._currentBatch().Should().BeNull();
            result._cursorId().Should().Be(cursorId);
            result._disposed().Should().BeFalse();
            result._firstBatch().Should().Equal(firstBatch);
            result._limit().Should().Be(limit);
            result._maxTime().Should().Be(maxTime);
            result._messageEncoderSettings().Should().BeEquivalentTo(messageEncoderSettings);
            result._query().Should().Be(query);
            result._serializer().Should().Be(serializer);
        }

        [Theory]
        [ParameterAttributeData]
        public void constructor_should_throw_when_batch_size_is_invalid(
            [Values(-1)]
            int value)
        {
            Action action = () => CreateSubject(batchSize: value);

            action.ShouldThrow<ArgumentException>().And.ParamName.Should().Be("batchSize");
        }

        [Fact]
        public void constructor_should_throw_when_collection_namespace_is_null()
        {
            Action action = () => CreateSubject(collectionNamespace: null);

            action.ShouldThrow<ArgumentNullException>().And.ParamName.Should().Be("collectionNamespace");
        }

        [Fact]
        public void constructor_should_throw_when_first_batch_is_null()
        {
            Action action = () => CreateSubject(firstBatch: null);

            action.ShouldThrow<ArgumentNullException>().And.ParamName.Should().Be("firstBatch");
        }

        [Theory]
        [ParameterAttributeData]
        public void constructor_should_throw_when_limit_is_invalid(
            [Values(-1)]
            int value)
        {
            Action action = () => CreateSubject(limit: value);

            action.ShouldThrow<ArgumentException>().And.ParamName.Should().Be("limit");
        }

        [Fact]
        public void constructor_should_throw_when_query_is_null()
        {
            Action action = () => CreateSubject(query: null);

            action.ShouldThrow<ArgumentNullException>().And.ParamName.Should().Be("query");
        }

        [Fact]
        public void constructor_should_throw_when_serializer_is_null()
        {
            Action action = () => CreateSubject(serializer: null);

            action.ShouldThrow<ArgumentNullException>().And.ParamName.Should().Be("serializer");
        }

        [Fact]
        public void CreateGetMoreCommand_should_return_expected_result()
        {
            var subject = CreateSubject();

            var result = subject.CreateGetMoreCommand();

            result.Should().Be("{ getMore : 0, collection : \"test\" }");
        }

        [Fact]
        public void CreateGetMoreCommand_should_return_expected_result_when_batchSize_is_provided()
        {
            var subject = CreateSubject(batchSize: 2);

            var result = subject.CreateGetMoreCommand();

            result.Should().Be("{ getMore : 0, collection : \"test\", batchSize : 2 }");
        }

        [Fact]
        public void CreateGetMoreCommand_should_return_expected_result_when_maxTime_is_provided()
        {
            var subject = CreateSubject(maxTime: TimeSpan.FromSeconds(2));

            var result = subject.CreateGetMoreCommand();

            result.Should().Be("{ getMore : 0, collection : \"test\", maxTimeMS : 2000 }");
        }

        [Fact]
        public void Dispose_should_dispose_channel_source_when_cursor_id_is_zero()
        {
            var mockChannelSource = new Mock<IChannelSource>();
            var subject = CreateSubject(cursorId: 1, channelSource: Optional.Create(mockChannelSource.Object));

            subject.Dispose();

            mockChannelSource.Verify(s => s.Dispose(), Times.Once);
        }

        [Theory]
        [ParameterAttributeData]
        public void GetMore_should_use_same_session(
            [Values(false, true)] bool async)
        {
            var mockChannelSource = new Mock<IChannelSource>();
            var channelSource = mockChannelSource.Object;
            var mockChannel = new Mock<IChannelHandle>();
            var channel = mockChannel.Object;
            var mockSession = new Mock<ICoreSessionHandle>();
            var session = mockSession.Object;
            var databaseNamespace = new DatabaseNamespace("database");
            var collectionNamespace = new CollectionNamespace(databaseNamespace, "collection");
            var cursorId = 1;
            var subject = CreateSubject(collectionNamespace: collectionNamespace, cursorId: cursorId, channelSource: Optional.Create(channelSource));
            var cancellationToken = new CancellationTokenSource().Token;
            var connectionDescription = CreateConnectionDescriptionSupportingSession();

            mockChannelSource.SetupGet(m => m.Session).Returns(session);
            mockChannel.SetupGet(m => m.ConnectionDescription).Returns(connectionDescription);
            var nextBatchBytes = new byte[] { 5, 0, 0, 0, 0 };
            var nextBatchSlice = new ByteArrayBuffer(nextBatchBytes, isReadOnly: true);
            var secondBatch = new BsonDocument
            {
                { "cursor", new BsonDocument
                    {
                        { "id", 0 },
                        { "nextBatch", new RawBsonArray(nextBatchSlice) }
                    }
                }
            };

            subject.MoveNext(cancellationToken); // skip empty first batch
            var sameSessionWasUsed = false;
            if (async)
            {
                mockChannelSource.Setup(m => m.GetChannelAsync(cancellationToken)).Returns(Task.FromResult(channel));
                mockChannel
                    .Setup(m => m.CommandAsync(
                        session,
                        null,
                        databaseNamespace,
                        It.IsAny<BsonDocument>(),
                        NoOpElementNameValidator.Instance,
                        null,
                        It.IsAny<Func<CommandResponseHandling>>(),
                        It.IsAny<IBsonSerializer<BsonDocument>>(),
                        It.IsAny<MessageEncoderSettings>(),
                        cancellationToken))
                    .Callback(() => sameSessionWasUsed = true)
                    .Returns(Task.FromResult(secondBatch));

                subject.MoveNextAsync(cancellationToken).GetAwaiter().GetResult();
            }
            else
            {
                mockChannelSource.Setup(m => m.GetChannel(cancellationToken)).Returns(channel);
                mockChannel
                    .Setup(m => m.Command(
                        session,
                        null,
                        databaseNamespace,
                        It.IsAny<BsonDocument>(),
                        NoOpElementNameValidator.Instance,
                        null,
                        It.IsAny<Func<CommandResponseHandling>>(),
                        It.IsAny<IBsonSerializer<BsonDocument>>(),
                        It.IsAny<MessageEncoderSettings>(),
                        cancellationToken))
                    .Callback(() => sameSessionWasUsed = true)
                    .Returns(secondBatch);

                subject.MoveNext(cancellationToken);
            }

            sameSessionWasUsed.Should().BeTrue();
        }

        // private methods
        private ConnectionDescription CreateConnectionDescriptionSupportingSession()
        {
            var clusterId = new ClusterId(1);
            var endPoint = new DnsEndPoint("localhost", 27017);
            var serverId = new ServerId(clusterId, endPoint);
            var connectionId = new ConnectionId(serverId, 1);
            var isMasterDocument = new BsonDocument
            {
                { "logicalSessionTimeoutMinutes", 30 }
            };
            var isMasterResult = new IsMasterResult(isMasterDocument);
            var buildInfoDocument = new BsonDocument
            {
                { "version", "3.6.0" }
            };
            var buildInfoResult = new BuildInfoResult(buildInfoDocument);
            return new ConnectionDescription(connectionId, isMasterResult, buildInfoResult);
        }

        private AsyncCursor<BsonDocument> CreateSubject(
            Optional<IChannelSource> channelSource = default(Optional<IChannelSource>),
            Optional<CollectionNamespace> collectionNamespace = default(Optional<CollectionNamespace>),
            Optional<IBsonSerializer<BsonDocument>> serializer = default(Optional<IBsonSerializer<BsonDocument>>),
            Optional<BsonDocument> query = default(Optional<BsonDocument>),
            Optional<IReadOnlyList<BsonDocument>> firstBatch = default(Optional<IReadOnlyList<BsonDocument>>),
            Optional<long> cursorId = default(Optional<long>),
            Optional<int?> batchSize = default(Optional<int?>),
            Optional<int?> limit = default(Optional<int?>),
            Optional<TimeSpan?> maxTime = default(Optional<TimeSpan?>))
        {
            return new AsyncCursor<BsonDocument>(
                channelSource.WithDefault(new Mock<IChannelSource>().Object),
                collectionNamespace.WithDefault(new CollectionNamespace("test", "test")),
                query.WithDefault(new BsonDocument()),
                firstBatch.WithDefault(new List<BsonDocument>()),
                cursorId.WithDefault(0),
                batchSize.WithDefault(null),
                limit.WithDefault(null),
                serializer.WithDefault(BsonDocumentSerializer.Instance),
                new MessageEncoderSettings(),
                maxTime.WithDefault(null));
        }
    }

    public class AsyncCursorIntegrationTests : OperationTestBase
    {
        [Theory]
        [InlineData(0, 1000)]
        [InlineData(2, 2)]
        [InlineData(2, 1000)]
        [InlineData(4, 2)]
        [InlineData(4, 4)]
        [InlineData(4, 1000)]
        public void Session_reference_count_should_be_decremented_as_soon_as_possible(int collectionSize, int batchSize)
        {
            RequireServer.Check();
            DropCollection();
            var documents = Enumerable.Range(1, collectionSize).Select(n => new BsonDocument("_id", n));
            Insert(documents);

            var findOperation = new FindOperation<BsonDocument>(_collectionNamespace, BsonDocumentSerializer.Instance, _messageEncoderSettings)
            {
                BatchSize = batchSize
            };

            _session.ReferenceCount().Should().Be(1);
            using (var binding = new WritableServerBinding(_cluster, _session.Fork()))
            using (var cursor = findOperation.Execute(binding, CancellationToken.None))
            {
                AssertExpectedSessionReferenceCount(_session, cursor);
                while (cursor.MoveNext())
                {
                    AssertExpectedSessionReferenceCount(_session, cursor);
                }
                AssertExpectedSessionReferenceCount(_session, cursor);
            }
            _session.ReferenceCount().Should().Be(1);
        }

        // private methods
        private void AssertExpectedSessionReferenceCount(ICoreSessionHandle session, IAsyncCursor<BsonDocument> cursor)
        {
            var cursorImplementation = (AsyncCursor<BsonDocument>)cursor;
            var cursorId = cursorImplementation._cursorId();
            var expectedReferenceCount = cursorId == 0 ? 2 : 3; // one from the session, one from the binding, and maybe one from the cursor
            session.ReferenceCount().Should().Be(expectedReferenceCount);
        }
    }

    public static class AsyncCursorReflector
    {
        public static int? _batchSize(this AsyncCursor<BsonDocument> obj)
        {
            var fieldInfo = obj.GetType().GetField("_batchSize", BindingFlags.NonPublic | BindingFlags.Instance);
            return (int?)fieldInfo.GetValue(obj);
        }

        public static IChannelSource _channelSource(this AsyncCursor<BsonDocument> obj)
        {
            var fieldInfo = obj.GetType().GetField("_channelSource", BindingFlags.NonPublic | BindingFlags.Instance);
            return (IChannelSource)fieldInfo.GetValue(obj);
        }

        public static CollectionNamespace _collectionNamespace(this AsyncCursor<BsonDocument> obj)
        {
            var fieldInfo = obj.GetType().GetField("_collectionNamespace", BindingFlags.NonPublic | BindingFlags.Instance);
            return (CollectionNamespace)fieldInfo.GetValue(obj);
        }

        public static int _count(this AsyncCursor<BsonDocument> obj)
        {
            var fieldInfo = obj.GetType().GetField("_count", BindingFlags.NonPublic | BindingFlags.Instance);
            return (int)fieldInfo.GetValue(obj);
        }

        public static IReadOnlyList<BsonDocument> _currentBatch(this AsyncCursor<BsonDocument> obj)
        {
            var fieldInfo = obj.GetType().GetField("_currentBatch", BindingFlags.NonPublic | BindingFlags.Instance);
            return (IReadOnlyList<BsonDocument>)fieldInfo.GetValue(obj);
        }

        public static long _cursorId(this AsyncCursor<BsonDocument> obj)
        {
            var fieldInfo = obj.GetType().GetField("_cursorId", BindingFlags.NonPublic | BindingFlags.Instance);
            return (long)fieldInfo.GetValue(obj);
        }

        public static bool _disposed(this AsyncCursor<BsonDocument> obj)
        {
            var fieldInfo = obj.GetType().GetField("_disposed", BindingFlags.NonPublic | BindingFlags.Instance);
            return (bool)fieldInfo.GetValue(obj);
        }

        public static IReadOnlyList<BsonDocument> _firstBatch(this AsyncCursor<BsonDocument> obj)
        {
            var fieldInfo = obj.GetType().GetField("_firstBatch", BindingFlags.NonPublic | BindingFlags.Instance);
            return (IReadOnlyList<BsonDocument>)fieldInfo.GetValue(obj);
        }

        public static int _limit(this AsyncCursor<BsonDocument> obj)
        {
            var fieldInfo = obj.GetType().GetField("_limit", BindingFlags.NonPublic | BindingFlags.Instance);
            return (int)fieldInfo.GetValue(obj);
        }

        public static TimeSpan? _maxTime(this AsyncCursor<BsonDocument> obj)
        {
            var fieldInfo = obj.GetType().GetField("_maxTime", BindingFlags.NonPublic | BindingFlags.Instance);
            return (TimeSpan?)fieldInfo.GetValue(obj);
        }

        public static MessageEncoderSettings _messageEncoderSettings(this AsyncCursor<BsonDocument> obj)
        {
            var fieldInfo = obj.GetType().GetField("_messageEncoderSettings", BindingFlags.NonPublic | BindingFlags.Instance);
            return (MessageEncoderSettings)fieldInfo.GetValue(obj);
        }

        public static BsonDocument _query(this AsyncCursor<BsonDocument> obj)
        {
            var fieldInfo = obj.GetType().GetField("_query", BindingFlags.NonPublic | BindingFlags.Instance);
            return (BsonDocument)fieldInfo.GetValue(obj);
        }

        public static IBsonSerializer<BsonDocument> _serializer(this AsyncCursor<BsonDocument> obj)
        {
            var fieldInfo = obj.GetType().GetField("_serializer", BindingFlags.NonPublic | BindingFlags.Instance);
            return (IBsonSerializer<BsonDocument>)fieldInfo.GetValue(obj);
        }

        public static BsonDocument CreateGetMoreCommand(this AsyncCursor<BsonDocument> obj)
        {
            var methodInfo = obj.GetType().GetMethod("CreateGetMoreCommand", BindingFlags.NonPublic | BindingFlags.Instance);
            return (BsonDocument)methodInfo.Invoke(obj, new object[0]);
        }
    }
}
