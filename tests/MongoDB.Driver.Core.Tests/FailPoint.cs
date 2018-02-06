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
using System.Linq;
using System.Threading;
using System.Xml.Schema;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Clusters.ServerSelectors;
using MongoDB.Driver.Core.Operations;
using MongoDB.Driver.Core.Servers;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;

namespace MongoDB.Driver.Core.Tests
{
    public static class FailPointName
    {
        // public constants
        public const string MaxTimeAlwaysTimeout = "maxTimeAlwaysTimeOut";
        public const string OnPrimaryTransactionalWrite = "onPrimaryTransactionalWrite";
    }

    public class FailPoint
    {
        // private readonly fields
        private readonly string _name;
        private readonly MessageEncoderSettings _messageEncoderSettings;
        private readonly ICoreSessionHandle _session;
        private readonly Lazy<IServer> _server;

        

        // constructors
        public FailPoint(string name, ICluster cluster, ICoreSessionHandle session, MessageEncoderSettings messageEncoderSettings)
        {
            if (name == null) throw new ArgumentNullException(nameof(name)); 
            if (cluster == null) throw new ArgumentNullException(nameof(cluster)); 
            if (session == null) throw new ArgumentNullException(nameof(session));
            if (messageEncoderSettings == null) throw new ArgumentNullException(nameof(messageEncoderSettings));

            _name = name;
            _messageEncoderSettings = messageEncoderSettings;
            _server = new Lazy<IServer>(()=> GetWriteableServer(cluster));
            _session = session;
        }

        /// <summary>
        /// Creates a context in which the FailPoint is always on. 
        /// </summary>
        public IFailPointContext CreateAlwaysOnContext()
        {
            return CreateFailPointContext(mode: "alwaysOn");
        }

        /// <summary>
        /// Create a context in which the FailPoint occurs a set number of times. 
        /// </summary>
        /// <param name="n">The number of times the FailPoint happen.</param>
        public IFailPointContext CreateSetTimesContext(int n)
        {
            return CreateFailPointContext(mode: new BsonDocument("times", n));
        }

        // <summary>
        /// Whether or not the FailPoint will work on the cluster.
        /// </summary>
        /// <value>Whether or not the FailPoint is supported.</value>
        public bool IsThisFailPointSupported()
        {
            // some failpoints aren't supported everywhere
            switch (_name)
            {
                case FailPointName.MaxTimeAlwaysTimeout:
                    return _server.Value.Description.Type != ServerType.ShardRouter;
                default:
                    return true;
            }
        }

        private FailPointContext CreateFailPointContext(BsonValue mode)
        {
            return new FailPointContext(
                binding => SetMode(mode, binding),
                binding => SetMode("off", binding),
                () => new SingleServerReadWriteBinding(_server.Value, _session)); 
        }

       

        private IServer GetWriteableServer(ICluster cluster)
        {
            var selector = WritableServerSelector.Instance;
            return cluster.SelectServer(selector, CancellationToken.None);
        }

        private void SetMode(BsonValue mode, SingleServerReadWriteBinding binding)
        {
            var command = new BsonDocument
            {
                { "configureFailPoint", _name },
                { "mode", mode }
            };

            var operation = new WriteCommandOperation<BsonDocument>(
                databaseNamespace: new DatabaseNamespace("admin"),
                command: command,
                resultSerializer: new BsonDocumentSerializer(),
                messageEncoderSettings: _messageEncoderSettings);

            operation.Execute(binding, CancellationToken.None);
        }


        private class FailPointContext : IFailPointContext
        {
            private readonly Action<SingleServerReadWriteBinding> _failPointOff;

            private bool _disposed;
            
            public FailPointContext(
                Action<SingleServerReadWriteBinding> failPointOn, 
                Action<SingleServerReadWriteBinding> failPointOff,
                Func<SingleServerReadWriteBinding> createBinding)
            {
                _failPointOff = failPointOff;
                Binding = createBinding();

                failPointOn(Binding);
            }

            public void Dispose()
            {
                if (_disposed) return;
                try { _failPointOff(Binding); }
                finally
                {
                    _disposed = true;
                    Binding.Dispose();
                }
            }

            public SingleServerReadWriteBinding Binding { get; }
        }

        public interface IFailPointContext : IDisposable
        {
            SingleServerReadWriteBinding Binding { get; }
        }
    }

    
}
