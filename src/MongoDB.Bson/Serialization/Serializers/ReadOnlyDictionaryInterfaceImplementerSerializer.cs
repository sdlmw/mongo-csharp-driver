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
using System.Collections.Generic;
using MongoDB.Bson.Serialization.Options;

namespace MongoDB.Bson.Serialization.Serializers
{
    /// <summary>
    /// Represents a serializer for a class that implements <see cref="IReadOnlyDictionary{TKey, TValue}"/>.
    /// </summary>
    /// <typeparam name="TReadOnlyDictionary">The type of the dictionary.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public class ReadOnlyDictionaryInterfaceImplementerSerializer<TReadOnlyDictionary, TKey, TValue> :
        DictionarySerializerBase<TReadOnlyDictionary, TKey, TValue>,
        IChildSerializerConfigurable,
        IDictionaryRepresentationConfigurable<ReadOnlyDictionaryInterfaceImplementerSerializer<TReadOnlyDictionary, TKey, TValue>>
            where TReadOnlyDictionary : class, IReadOnlyDictionary<TKey, TValue>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ReadOnlyDictionaryInterfaceImplementerSerializer{TReadOnlyDictionary, TKey, TValue}"/> class.
        /// </summary>
        public ReadOnlyDictionaryInterfaceImplementerSerializer()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ReadOnlyDictionaryInterfaceImplementerSerializer{TReadOnlyDictionary, TKey, TValue}"/> class.
        /// </summary>
        /// <param name="dictionaryRepresentation">The dictionary representation.</param>
        public ReadOnlyDictionaryInterfaceImplementerSerializer(DictionaryRepresentation dictionaryRepresentation)
            : base(dictionaryRepresentation)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ReadOnlyDictionaryInterfaceImplementerSerializer{TReadOnlyDictionary, TKey, TValue}"/> class.
        /// </summary>
        /// <param name="dictionaryRepresentation">The dictionary representation.</param>
        /// <param name="keySerializer">The key serializer.</param>
        /// <param name="valueSerializer">The value serializer.</param>
        public ReadOnlyDictionaryInterfaceImplementerSerializer(DictionaryRepresentation dictionaryRepresentation, IBsonSerializer<TKey> keySerializer, IBsonSerializer<TValue> valueSerializer)
            : base(dictionaryRepresentation, keySerializer, valueSerializer)
        {
        }

        // public methods
        /// <summary>
        /// Returns a serializer that has been reconfigured with the specified dictionary representation.
        /// </summary>
        /// <param name="dictionaryRepresentation">The dictionary representation.</param>
        /// <returns>The reconfigured serializer.</returns>
        public ReadOnlyDictionaryInterfaceImplementerSerializer<TReadOnlyDictionary, TKey, TValue> WithDictionaryRepresentation(DictionaryRepresentation dictionaryRepresentation)
        {
            if (dictionaryRepresentation == DictionaryRepresentation)
            {
                return this;
            }
            else
            {
                return new ReadOnlyDictionaryInterfaceImplementerSerializer<TReadOnlyDictionary, TKey, TValue>(dictionaryRepresentation, KeySerializer, ValueSerializer);
            }
        }

        /// <summary>
        /// Returns a serializer that has been reconfigured with the specified dictionary representation and key value serializers.
        /// </summary>
        /// <param name="dictionaryRepresentation">The dictionary representation.</param>
        /// <param name="keySerializer">The key serializer.</param>
        /// <param name="valueSerializer">The value serializer.</param>
        /// <returns>The reconfigured serializer.</returns>
        public ReadOnlyDictionaryInterfaceImplementerSerializer<TReadOnlyDictionary, TKey, TValue> WithDictionaryRepresentation(DictionaryRepresentation dictionaryRepresentation, IBsonSerializer<TKey> keySerializer, IBsonSerializer<TValue> valueSerializer)
        {
            if (dictionaryRepresentation == DictionaryRepresentation && keySerializer == KeySerializer && valueSerializer == ValueSerializer)
            {
                return this;
            }
            else
            {
                return new ReadOnlyDictionaryInterfaceImplementerSerializer<TReadOnlyDictionary, TKey, TValue>(dictionaryRepresentation, keySerializer, valueSerializer);
            }
        }

        /// <summary>
        /// Returns a serializer that has been reconfigured with the specified key serializer.
        /// </summary>
        /// <param name="keySerializer">The key serializer.</param>
        /// <returns>The reconfigured serializer.</returns>
        public ReadOnlyDictionaryInterfaceImplementerSerializer<TReadOnlyDictionary, TKey, TValue> WithKeySerializer(IBsonSerializer<TKey> keySerializer)
        {
            if (keySerializer == KeySerializer)
            {
                return this;
            }
            else
            {
                return new ReadOnlyDictionaryInterfaceImplementerSerializer<TReadOnlyDictionary, TKey, TValue>(DictionaryRepresentation, keySerializer, ValueSerializer);
            }
        }

        /// <summary>
        /// Returns a serializer that has been reconfigured with the specified value serializer.
        /// </summary>
        /// <param name="valueSerializer">The value serializer.</param>
        /// <returns>The reconfigured serializer.</returns>
        public ReadOnlyDictionaryInterfaceImplementerSerializer<TReadOnlyDictionary, TKey, TValue> WithValueSerializer(IBsonSerializer<TValue> valueSerializer)
        {
            if (valueSerializer == ValueSerializer)
            {
                return this;
            }
            else
            {
                return new ReadOnlyDictionaryInterfaceImplementerSerializer<TReadOnlyDictionary, TKey, TValue>(DictionaryRepresentation, KeySerializer, valueSerializer);
            }
        }

        // protected methods
        /// <inheritdoc />
        protected override ICollection<KeyValuePair<TKey, TValue>> CreateAccumulator()
        {
            return new Dictionary<TKey, TValue>();
        }

        /// <inheritdoc />
        protected override TReadOnlyDictionary Finalize(ICollection<KeyValuePair<TKey, TValue>> accumulator)
        {
            var dictionary = (Dictionary<TKey, TValue>)accumulator;
            return (TReadOnlyDictionary)Activator.CreateInstance(typeof(TReadOnlyDictionary), new object[] { dictionary });
        }

        // explicit interface implementations
        IBsonSerializer IChildSerializerConfigurable.ChildSerializer
        {
            get { return ValueSerializer; }
        }

        IBsonSerializer IChildSerializerConfigurable.WithChildSerializer(IBsonSerializer childSerializer)
        {
            return WithValueSerializer((IBsonSerializer<TValue>)childSerializer);
        }

        IBsonSerializer IDictionaryRepresentationConfigurable.WithDictionaryRepresentation(DictionaryRepresentation dictionaryRepresentation)
        {
            return WithDictionaryRepresentation(dictionaryRepresentation);
        }
    }
}
