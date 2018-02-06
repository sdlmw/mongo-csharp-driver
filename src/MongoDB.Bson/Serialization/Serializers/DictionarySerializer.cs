using System.Collections.Generic;
using MongoDB.Bson.Serialization.Options;

namespace MongoDB.Bson.Serialization.Serializers
{
    /// <summary>
    /// Represents a serializer for the class <see cref="Dictionary{TKey, TValue}"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public class DictionarySerializer<TKey, TValue> :
        DictionaryInterfaceImplementerSerializer<Dictionary<TKey, TValue>, TKey, TValue>
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="DictionarySerializer{TKey,TValue}"/> class.
        /// </summary>
        public DictionarySerializer()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DictionarySerializer{TKey, TValue}"/> class.
        /// </summary>
        /// <param name="dictionaryRepresentation">The dictionary representation.</param>
        public DictionarySerializer(DictionaryRepresentation dictionaryRepresentation)
            : base(dictionaryRepresentation)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DictionarySerializer{TKey, TValue}"/> class.
        /// </summary>
        /// <param name="dictionaryRepresentation">The dictionary representation.</param>
        /// <param name="keySerializer">The key serializer.</param>
        /// <param name="valueSerializer">The value serializer.</param>
        public DictionarySerializer(DictionaryRepresentation dictionaryRepresentation, IBsonSerializer<TKey> keySerializer, IBsonSerializer<TValue> valueSerializer)
            : base(dictionaryRepresentation, keySerializer, valueSerializer)
        {
        }
    }
}