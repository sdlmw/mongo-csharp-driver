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
using MongoDB.Driver.Core.Misc;
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

    public static class FailPointMode
    {
        /// <summary>
        /// A mode in which the FailPoint is always on. 
        /// </summary>
        public static BsonValue AlwaysOn => "alwaysOn";
        
        /// <summary>
        /// Create a mode in which the FailPoint occurs a set number of times. 
        /// </summary>
        /// <param name="n">The number of times the FailPoint happen.</param>
        public static BsonValue ConfigureTimes(int n)
        {
            return new BsonDocument("times", n);
        }
    }

    public class FailPoint
    {
        // private readonly fields
        private readonly string _name;
        private readonly MessageEncoderSettings _messageEncoderSettings;
        private readonly BsonValue _mode;
        private readonly ICoreSessionHandle _session;
        private readonly Lazy<IServer> _server;
        
        // constructors
        public FailPoint(string name, BsonValue mode, ICluster cluster, ICoreSessionHandle session, MessageEncoderSettings messageEncoderSettings)
        {
            Ensure.IsNotNull(name, nameof(name));
            Ensure.IsNotNull(cluster, nameof(cluster));
            Ensure.IsNotNull(messageEncoderSettings, nameof(messageEncoderSettings));
   
            _name = name;
            _mode = mode;
            _messageEncoderSettings = messageEncoderSettings;
            _server = new Lazy<IServer>(()=> GetWriteableServer(cluster));
            _session = session;
        }

        /// <summary>
        /// Creates a context with a custom mode
        /// </summary>
        public IFailPointContext CreateContext()
        {
            return new FailPointContext(
                binding => SendFailPointCommand(_mode, binding),
                binding => SendFailPointCommand("off", binding),
                () => new SingleServerReadWriteBinding(_server.Value, _session));
        }

        // /<summary>
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
       
        private IServer GetWriteableServer(ICluster cluster)
        {
            var selector = WritableServerSelector.Instance;
            return cluster.SelectServer(selector, CancellationToken.None);
        }

        private void SendFailPointCommand(BsonValue mode, SingleServerReadWriteBinding binding)
        {
            var command = new BsonDocument
            {
                { "configureFailPoint", _name },
                { "mode", mode }
            };

            var operation = new WriteCommandOperation<BsonDocument>(
                databaseNamespace: new DatabaseNamespace("admin"),
                command: command,
                resultSerializer: BsonDocumentSerializer.Instance,
                messageEncoderSettings: _messageEncoderSettings);

            operation.Execute(binding, CancellationToken.None);
        }


        private class FailPointContext : IFailPointContext
        {
            private readonly Lazy<SingleServerReadWriteBinding> _binding;
            private readonly Action<SingleServerReadWriteBinding> _failPointOn;
            private readonly Action<SingleServerReadWriteBinding> _failPointOff;

            private bool _disposed;
            private bool _wasEnabled = false;

            public SingleServerReadWriteBinding Binding => _binding.Value;

            public FailPointContext(
                Action<SingleServerReadWriteBinding> failPointOn, 
                Action<SingleServerReadWriteBinding> failPointOff,
                Func<SingleServerReadWriteBinding> createBinding)
            {
                _failPointOn = failPointOn;
                _failPointOff = failPointOff;
                _binding = new Lazy<SingleServerReadWriteBinding>(createBinding);
                failPointOn(Binding);
            }

            /// <summary>
            /// Turns on the FailPoint, sending the command to the server
            /// </summary>
            public void Enable()
            {
                  _failPointOn(_binding.Value);
                  _wasEnabled = true;
            }

            public void Dispose()
            {
                if (_disposed) return;
                if (_wasEnabled) _failPointOff(Binding); 
                _disposed = true;
                Binding.Dispose();
            }
        }

        public interface IFailPointContext : IDisposable
        {
            SingleServerReadWriteBinding Binding { get; }
            void Enable();
        }
    }

    
}
