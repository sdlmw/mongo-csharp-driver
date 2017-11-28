using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace MongoDB.Driver.Core.Misc
{
    /// <summary>
    /// Represents a serializer for splittable batches that serializes as much of the splittable batch as fits in the max message size.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SplittableBatchSerializer<T> : SealedClassSerializerBase<SplittableBatch<T>>
    {
        // private fields
        private readonly int _maxMessageSize;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="SplittableBatchSerializer{T}"/> class.
        /// </summary>
        /// <param name="maxMessageSize">Maximum size of the message.</param>
        public SplittableBatchSerializer(int maxMessageSize)
        {
            _maxMessageSize = Ensure.IsBetween(maxMessageSize, 1, int.MaxValue, nameof(maxMessageSize));
        }

        // public methods
        /// <inheritdoc />
        protected override void SerializeValue(BsonSerializationContext context, BsonSerializationArgs args, SplittableBatch<T> value)
        {
            var writer = context.Writer;
            while (writer is WrappingBsonWriter)
            {
                writer = ((WrappingBsonWriter)writer).Wrapped;
            }
            

        }
    }
}
