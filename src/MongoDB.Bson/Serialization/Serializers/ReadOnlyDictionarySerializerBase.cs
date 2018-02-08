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
    public class ReadOnlyDictionarySerializer<TKey, TValue> : DictionarySerializerBase<ReadOnlyDictionary<TKey, TValue>, TKey, TValue>
    {
        /// <summary>
        /// Not implemented for ReadOnlyDictionarySerializers
        /// </summary>
        /// <returns></returns>
        protected override ReadOnlyDictionary<TKey, TValue> CreateInstance() { throw new NotImplementedException(); }
        // => new ReadOnlyDictionary<TKey, TValue>(new Dictionary<TKey, TValue>());

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public override ReadOnlyDictionary<TKey, TValue> Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            var dict = BsonSerializer.Deserialize<Dictionary<TKey, TValue>>(context.Reader);
            return dict != null ? new ReadOnlyDictionary<TKey, TValue>(dict) : null;
        }
    }

}

