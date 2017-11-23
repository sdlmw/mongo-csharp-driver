using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver.Core.Operations
{
    /// <summary>
    /// Represents a context for retryable writes.
    /// </summary>
    /// <seealso cref="System.IDisposable" />
    public sealed class RetryableWriteContext : IDisposable
    {
        // private fields
        private readonly IWriteBinding _binding;
        private IChannelHandle _channel;
        private IChannelSourceHandle _channelSource;
        private bool _disposed;
        private readonly bool _withRetry;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="RetryableWriteContext"/> class.
        /// </summary>
        /// <param name="binding">The binding.</param>
        /// <param name="withRetry">if set to <c>true</c> [with retry].</param>
        public RetryableWriteContext(IWriteBinding binding, bool withRetry)
        {
            _binding = Ensure.IsNotNull(binding, nameof(binding));
            _withRetry = withRetry;
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
        public bool WithRetry => _withRetry;

        // public methods
        /// <inheritdoc />
        public void Dispose()
        {
            if (!_disposed)
            {
                _channelSource?.Dispose();
                _channel?.Dispose();
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
