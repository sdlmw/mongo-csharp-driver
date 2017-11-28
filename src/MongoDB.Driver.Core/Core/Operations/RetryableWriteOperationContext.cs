/* Copyright 2017 MongoDB Inc.
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
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core.Operations
{
    /// <summary>
    /// Represents a context for retryable writes.
    /// </summary>
    /// <seealso cref="System.IDisposable" />
    public sealed class RetryableWriteOperationContext : IDisposable
    {
        // private fields
        private readonly IWriteBinding _binding;
        private IChannelHandle _channel;
        private IChannelSourceHandle _channelSource;
        private bool _disposed;
        private readonly bool _retryable;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="RetryableWriteOperationContext"/> class.
        /// </summary>
        /// <param name="binding">The binding.</param>
        /// <param name="retryable">if set to <c>true</c> the operation can be retried.</param>
        public RetryableWriteOperationContext(IWriteBinding binding, bool retryable)
        {
            _binding = Ensure.IsNotNull(binding, nameof(binding));
            _retryable = retryable;
        }

        // public properties
        /// <summary>
        /// Gets the binding.
        /// </summary>
        /// <value>
        /// The binding.
        /// </value>
        public IWriteBinding Binding => _binding;

        /// <summary>
        /// Gets the channel.
        /// </summary>
        /// <value>
        /// The channel.
        /// </value>
        public IChannelHandle Channel => _channel;

        /// <summary>
        /// Gets the channel source.
        /// </summary>
        /// <value>
        /// The channel source.
        /// </value>
        public IChannelSourceHandle ChannelSource => _channelSource;

        /// <summary>
        /// Gets a value indicating whether [with retry].
        /// </summary>
        /// <value>
        ///   <c>true</c> if [with retry]; otherwise, <c>false</c>.
        /// </value>
        public bool Retryable => _retryable;

        // public methods
        /// <inheritdoc />
        public void Dispose()
        {
            if (!_disposed)
            {
                _channelSource?.Dispose();
                _channel?.Dispose();
                _disposed = true;
            }
        }

        /// <summary>
        /// Sets the channel.
        /// </summary>
        /// <param name="channel">The channel.</param>
        public void SetChannel(IChannelHandle channel)
        {
            Ensure.IsNotNull(channel, nameof(channel));
            _channel?.Dispose();
            _channel = channel;
        }

        /// <summary>
        /// Sets the channel source.
        /// </summary>
        /// <param name="channelSource">The channel source.</param>
        public void SetChannelSource(IChannelSourceHandle channelSource)
        {
            Ensure.IsNotNull(channelSource, nameof(channelSource));
            _channelSource?.Dispose();
            _channel?.Dispose();
            _channelSource = channelSource;
            _channel = null;
        }
    }
}
