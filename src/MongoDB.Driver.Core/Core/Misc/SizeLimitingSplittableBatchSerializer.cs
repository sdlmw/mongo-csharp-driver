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
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace MongoDB.Driver.Core.Misc
{
    /// <summary>
    /// A serializer for splittable batches that serializes as much of the splittable batch as fits in the max batch size.
    /// </summary>
    /// <typeparam name="TItem">The type of the items.</typeparam>
    public class SizeLimitingSplittableBatchSerializer<TItem> : SerializerBase<SplittableBatch<TItem>>
    {
        // private fields
        private readonly IElementNameValidator _itemElementNameValidator;
        private readonly IBsonSerializer<TItem> _itemSerializer;
        private readonly int _maxBatchSize;
        private readonly int _maxItemSize;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="SizeLimitingSplittableBatchSerializer{T}" /> class.
        /// </summary>
        /// <param name="itemSerializer">The item serializer.</param>
        /// <param name="itemElementNameValidator">The item element name validator.</param>
        /// <param name="maxItemSize">Maximum size of a serialized item.</param>
        /// <param name="maxBatchSize">Maximum size of the batch.</param>
        public SizeLimitingSplittableBatchSerializer(IBsonSerializer<TItem> itemSerializer, IElementNameValidator itemElementNameValidator, int maxItemSize, int maxBatchSize)
        {
            _itemSerializer = Ensure.IsNotNull(itemSerializer, nameof(itemSerializer));
            _itemElementNameValidator = Ensure.IsNotNull(itemElementNameValidator, nameof(itemElementNameValidator));
            _maxItemSize = Ensure.IsBetween(maxItemSize, 1, int.MaxValue, nameof(maxItemSize));
            _maxBatchSize = Ensure.IsBetween(maxBatchSize, maxItemSize, int.MaxValue, nameof(maxBatchSize));
        }

        // public methods
        /// <inheritdoc />
        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, SplittableBatch<TItem> value)
        {
            Ensure.IsNotNull(value, nameof(value));

            var writer = context.Writer;
            while (writer is WrappingBsonWriter)
            {
                writer = ((WrappingBsonWriter)writer).Wrapped;
            }

            var binaryWriter = writer as BsonBinaryWriter;
            var startPosition = binaryWriter?.Position;

            writer.PushSettings(s => { var bs = s as BsonBinaryWriterSettings; if (bs != null) { bs.MaxDocumentSize = _maxItemSize; } });
            writer.PushElementNameValidator(_itemElementNameValidator);
            try
            {
                for (var i = 0; i < value.Items.Count; i++)
                {
                    var itemPosition = binaryWriter?.Position;

                    var item = value.Items.Array[value.Items.Offset + i];
                    _itemSerializer.Serialize(context, args, item);

                    var batchSize = binaryWriter?.Position - startPosition;
                    if (batchSize > _maxBatchSize)
                    {
                        if (value.CanBeSplit)
                        {
                            binaryWriter.BaseStream.Position = itemPosition.Value; // remove the last item
                            value.SplitAt(i);
                            return;
                        }
                        else
                        {
                            throw new ArgumentException("Batch is too large.");
                        }
                    }
                }

                value.SplitAt(value.Items.Count);
            }
            finally
            {
                writer.PopElementNameValidator();
                writer.PopSettings();
            }
        }
    }
}
