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

namespace MongoDB.Driver.Tests
{
    public static class FailPointName
    {
        // public constants
        public const string MaxTimeAlwaysTimeout = "maxTimeAlwaysTimeOut";
    }

    public class FailPoint : IDisposable
    {
        // private readonly fields
        private readonly SingleServerReadWriteBinding _binding;
        private readonly string _name;
        private readonly MessageEncoderSettings _messageEncoderSettings;
        private readonly ICoreSessionHandle _session;
        private readonly IServer _server;

        // private fields
        private bool _disposed;
        private bool _wasSet;

        /// <summary>
        /// Binding associated with the FailPoint.
        /// </summary>
        /// <value>The binding.</value>
        public SingleServerReadWriteBinding Binding => _binding;

        /// <summary>
        /// Whether or not the FailPoint will work on the cluster.
        /// </summary>
        /// <value>Whether or not the FailPoint is supported.</value>
        public bool Supported => IsThisFailPointSupported();

        // constructors
        public FailPoint(string name, ICluster cluster, ICoreSessionHandle session, MessageEncoderSettings messageEncoderSettings)
        {
            if (name == null) throw new ArgumentNullException(nameof(name)); 
            if (cluster == null) throw new ArgumentNullException(nameof(cluster)); 
            if (session == null) throw new ArgumentNullException(nameof(session));
            if (messageEncoderSettings == null) throw new ArgumentNullException(nameof(messageEncoderSettings));

            _name = name;
            _messageEncoderSettings = messageEncoderSettings;
            _server = GetWriteableServer(cluster);
            _session = session;
            
            _binding = new SingleServerReadWriteBinding(_server, session);
        }

        // public methods
        /// <summary>
        /// Dispose of the FailPoint, turning off the FailPoint on the server if it was set.
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;
            try
            {   
                if (_wasSet) SetMode("off");
            }
            finally
            {
                _disposed = true;
                _binding.Dispose();
            }
        }

        /// <summary>
        /// Set the FailPoint's mode to alwaysOn.
        /// </summary>
        public void SetAlwaysOn()
        {
            SetMode("alwaysOn");
            _wasSet = true;
        }

        /// <summary>
        /// Set the number of times the FailPoint should occur.
        /// </summary>
        /// <param name="n">The number of times the FailPoint happen.</param>
        public void SetTimes(int n)
        {
            SetMode(new BsonDocument("times", n));
            _wasSet = true;
        }

        private bool IsThisFailPointSupported()
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

        private void SetMode(BsonValue mode)
        {
            var command = new BsonDocument
            {
                { "configureFailPoint", _name },
                { "mode", mode }
            };
            // _adminDatabase.RunCommand(command);


            var operation = new WriteCommandOperation<BsonDocument>(
                databaseNamespace: new DatabaseNamespace("admin"), 
                command: command, 
                resultSerializer: new BsonDocumentSerializer(), 
                messageEncoderSettings: _messageEncoderSettings);

            operation.Execute(_binding, CancellationToken.None);
            
        }

        private IServer GetWriteableServer(ICluster cluster)
        {
            var selector = WritableServerSelector.Instance;
            return cluster.SelectServer(selector, CancellationToken.None);
        }
    }
}
