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
        public static BsonValue CreateTimesToFailMode(int n)
        {
            return new BsonDocument("times", n);
        }
    }

    public class FailPoint : IDisposable
    {
        #region Private readonly fields
        private readonly Lazy<SingleServerReadWriteBinding> _binding;
        private readonly MessageEncoderSettings _messageEncoderSettings;
        private readonly BsonValue _mode;
        private readonly string _name;
        private readonly ICoreSessionHandle _session;
        private readonly Lazy<IServer> _server;
        #endregion

        #region Private mutable fields
        private bool _disposed;
        private bool _wasEnabled;
        #endregion

        /// <summary>
        /// The binding used by the FailPoint and associated commands.
        /// </summary>
        /// <value>Lazily constructed binding for the FailPoint.</value>
        public SingleServerReadWriteBinding Binding => _binding.Value;

        /// <summary>
        /// Creates a new FailPoint.
        /// </summary>
        /// <param name="name">The name (type) of the FailPoint.</param>
        /// <param name="mode">The mode.</param>
        /// <param name="cluster">The cluster.</param>
        /// <param name="session">The session.</param>
        /// <param name="messageEncoderSettings">The message encoder settings.</param>
        public FailPoint(string name, BsonValue mode, ICluster cluster, ICoreSessionHandle session, MessageEncoderSettings messageEncoderSettings)
        {
            Ensure.IsNotNull(name, nameof(name));
            Ensure.IsNotNull(cluster, nameof(cluster));
            Ensure.IsNotNull(messageEncoderSettings, nameof(messageEncoderSettings));

            _binding = new Lazy<SingleServerReadWriteBinding>(() => new SingleServerReadWriteBinding(_server.Value, _session));
            _messageEncoderSettings = messageEncoderSettings;
            _mode = mode;
            _name = name;
            _server = new Lazy<IServer>(() => GetWriteableServer(cluster));
            _session = session;
        }

        #region Static Methods
        /// <summary>
        /// Creates a FailPoint that always times out.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="session">The session.</param>
        /// <param name="messageEncoderSettings">The message encoder settings.</param>
        /// <returns>A FailPoint that always times out.</returns>
        public static FailPoint CreateAlwaysTimesOutFailPoint(ICluster cluster, ICoreSessionHandle session, MessageEncoderSettings messageEncoderSettings)
        {
            return new FailPoint(FailPointName.MaxTimeAlwaysTimeout, FailPointMode.AlwaysOn, cluster, session, messageEncoderSettings);
        }

        /// <summary>
        /// Creates a FailPoint that times out <paramref name="n"/> times.
        /// </summary>
        /// <param name="n">The number of times to time out.</param>
        /// <param name="cluster">The cluster.</param>
        /// <param name="session">The session.</param>
        /// <param name="messageEncoderSettings">The message encoder settings.</param>
        /// <returns>A FailPoint that always times out.</returns>
        public static FailPoint CreateTimesOutNTimesFailPoint(int n, ICluster cluster, ICoreSessionHandle session, MessageEncoderSettings messageEncoderSettings)
        {
            return new FailPoint(FailPointName.MaxTimeAlwaysTimeout, FailPointMode.CreateTimesToFailMode(n), cluster, session, messageEncoderSettings);
        }
        #endregion


        #region Instance Methods
        /// <summary>
        /// Turns on the FailPoint, sending the command to the server
        /// </summary>
        public void Enable()
        {
            SendFailPointCommand(_mode, Binding);
            _wasEnabled = true;
        }

        public void Dispose()
        {
            if (_disposed) return;
            if (_wasEnabled) SendFailPointCommand("off", Binding);
            _disposed = true;
            Binding.Dispose();
        }

        /// <summary>
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
                {"configureFailPoint", _name},
                {"mode", mode}
            };

            var operation = new WriteCommandOperation<BsonDocument>(
                databaseNamespace: new DatabaseNamespace("admin"),
                command: command,
                resultSerializer: BsonDocumentSerializer.Instance,
                messageEncoderSettings: _messageEncoderSettings);

            operation.Execute(binding, CancellationToken.None);
        }
        #endregion
    }

}
