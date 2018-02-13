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
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Bson.Serialization.Serializers;

namespace MongoDB.Bson.Serialization.Serializers
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class ReadOnlyDictionaryInterfaceSerializer<TKey, TValue> : ClassSerializerBase<IReadOnlyDictionary<TKey,TValue>> {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public override IReadOnlyDictionary<TKey, TValue> Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            var dict = BsonSerializer.Deserialize<Dictionary<TKey, TValue>>(context.Reader);
            return dict != null ? new ReadOnlyDictionary<TKey, TValue>(dict) : null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="args"></param>
        /// <param name="value"></param>
        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, IReadOnlyDictionary<TKey, TValue> value)
        {
            var wrappedDict = value != null ? new ReadOnlyDictionaryInterfaceWrapper(value) : null;
            BsonSerializer.Serialize<IDictionary<TKey, TValue>>(bsonWriter: context.Writer, value: wrappedDict, configurator: null, args: args);
        }

        private class ReadOnlyDictionaryInterfaceWrapper : IDictionary<TKey, TValue>
        {
            private readonly Lazy<ICollection<TKey>> _keys;
            private IReadOnlyDictionary<TKey,TValue> _map;
            private readonly Lazy<ICollection<TValue>> _values;

            public ReadOnlyDictionaryInterfaceWrapper(IReadOnlyDictionary<TKey, TValue> map)
            {
                _map = map;
                _keys = new Lazy<ICollection<TKey>>(() => new Collection<TKey>(_map.Keys.ToList()));
                _values = new Lazy<ICollection<TValue>>(() => new Collection<TValue>(_map.Values.ToList()));
            }

            public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
            {
                return _map.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public void Add(KeyValuePair<TKey, TValue> item)
            {
                throw new NotImplementedException();
            }

            public void Clear()
            {
                throw new NotImplementedException();
            }

            public bool Contains(KeyValuePair<TKey, TValue> item)
            {
                return _map.Contains(item);
            }

            public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
            {
                foreach (var kvp in _map) array[arrayIndex++] = kvp;
            }

            public bool Remove(KeyValuePair<TKey, TValue> item)
            {
                throw new NotImplementedException();
            }

            public int Count => _map.Count;
            public bool IsReadOnly => true;
            public bool ContainsKey(TKey key)
            {
                return _map.ContainsKey(key);
            }

            public void Add(TKey key, TValue value)
            {
                throw new NotImplementedException();
            }

            public bool Remove(TKey key)
            {
                throw new NotImplementedException();
            }

            public bool TryGetValue(TKey key, out TValue value)
            {
                return _map.TryGetValue(key, out value);
            }

            public TValue this[TKey key]
            {
                get { return _map[key]; }
                set { throw new NotImplementedException(); }
            }

            public ICollection<TKey> Keys => _keys.Value;

            public ICollection<TValue> Values => _values.Value;
        }
    }

}

