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
using System.Threading;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Clusters.ServerSelectors;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Core.Operations;
using MongoDB.Driver.Core.Servers;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;

namespace MongoDB.Driver.Core.TestHelpers
{
    public static class FailPointName
    {
        // public constants
        public const string MaxTimeAlwaysTimeout = "maxTimeAlwaysTimeOut";
        public const string OnPrimaryTransactionalWrite = "onPrimaryTransactionalWrite";
    }

    public sealed class FailPoint : IDisposable
    {
        #region static
        // public static methods
        public static FailPoint Configure(ICluster cluster, ICoreSessionHandle session, string name, BsonDocument args)
        {
            var server = GetWriteableServer(cluster);
            var failpoint = new FailPoint(server, session, name, args);
            try
            {
                failpoint.Configure();
                return failpoint;
            }
            catch
            {
                try { failpoint.Dispose(); } catch { }
                throw;
            }
        }

        public static FailPoint ConfigureAlwaysOn(ICluster cluster, ICoreSessionHandle session, string name)
        {
            var args = new BsonDocument("mode", "alwaysOn");
            return Configure(cluster, session, name, args);
        }

        public static FailPoint ConfigureTimes(ICluster cluster, ICoreSessionHandle session, string name, int n)
        {
            var args = new BsonDocument("mode", new BsonDocument("times", n));
            return Configure(cluster, session, name, args);
        }

        // private static methods
        private static IServer GetWriteableServer(ICluster cluster)
        {
            var selector = WritableServerSelector.Instance;
            return cluster.SelectServer(selector, CancellationToken.None);
        }
        #endregion

        // private fields
        private readonly BsonDocument _args;
        private readonly IReadWriteBinding _binding;
        private bool _disposed;
        private readonly string _name;
        private readonly IServer _server;

        // constructors
        public FailPoint(IServer server, ICoreSessionHandle session, string name, BsonDocument args)
        {
            _server = Ensure.IsNotNull(server, nameof(server));
            Ensure.IsNotNull(session, nameof(session));
            _name = Ensure.IsNotNull(name, nameof(name));
            _args = Ensure.IsNotNull(args, nameof(args));

            _binding = new SingleServerReadWriteBinding(server, session.Fork());
        }

        // public properties
        public IReadWriteBinding Binding => _binding;

        // public methods
        public void Configure()
        {
            Configure(_args);
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                try { ConfigureOff(); } catch { }
                _binding.Dispose();
                _disposed = true;
            }
        }

        public bool IsSupported()
        {
            if (Feature.FailPoints.IsSupported(_server.Description.Version))
            {
                // some failpoints aren't supported everywhere
                switch (_name)
                {
                    case FailPointName.MaxTimeAlwaysTimeout:
                        return _server.Description.Type != ServerType.ShardRouter;

                    default:
                        return true;
                }
            }
            else
            {
                return false;
            }
        }

        // private methods
        private void Configure(BsonDocument args)
        {
            var command = new BsonDocument("configureFailPoint", _name);
            command.Merge(args);
            ExecuteCommand(command);
        }

        private void ConfigureOff()
        {
            var args = new BsonDocument("mode", "off");
            Configure(args);
        }

        private void ExecuteCommand(BsonDocument command)
        {
            var adminDatabase = new DatabaseNamespace("admin");
            var operation = new WriteCommandOperation<BsonDocument>(
                adminDatabase,
                command,
                BsonDocumentSerializer.Instance,
                new MessageEncoderSettings());
            operation.Execute(_binding, CancellationToken.None);
        }
    }
}
