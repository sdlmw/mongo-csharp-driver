/* Copyright 2016 MongoDB Inc.
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
using System.Linq;
using System.Reflection;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver
{
    /// <summary>
    /// Represents the result of a $facet stage with one facet.
    /// </summary>
    /// <typeparam name="TResult1">The result type of facet 1.</typeparam>
    public class AggregateFacetResult<TResult1>
    {
        internal static AggregateFacetResult<TResult1> Create(string[] names, object[] results)
        {
            return new AggregateFacetResult<TResult1>(
                names[0],
                (IEnumerable<TResult1>)results[0]);
        }

        internal static IBsonSerializer<AggregateFacetResult<TResult1>> CreateSerializer(
            IEnumerable<Tuple<string, IBsonSerializer>> facets)
        {
            return new AggregateFacetResultSerializer<AggregateFacetResult<TResult1>>(facets, Create);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResult{TResult1}"/> class.
        /// </summary>
        public AggregateFacetResult(
            string name1,
            IEnumerable<TResult1> result1)
        {
            Name1 = Ensure.IsNotNull(name1, nameof(name1));
            Result1 = Ensure.IsNotNull(result1, nameof(name1));
        }

        /// <summary>
        /// Gets the name of facet 1.
        /// </summary>
        public string Name1 { get; private set; }

        /// <summary>
        /// Gets the result of facet 1.
        /// </summary>
        public IEnumerable<TResult1> Result1 { get; private set; }
    }

    /// <summary>
    /// Represents the result of a $facet stage with two facets.
    /// </summary>
    /// <typeparam name="TResult1">The result type of facet 1.</typeparam>
    /// <typeparam name="TResult2">The result type of facet 2.</typeparam>
    public class AggregateFacetResult<TResult1, TResult2>
    {
        internal static AggregateFacetResult<TResult1, TResult2> Create(string[] names, object[] results)
        {
            return new AggregateFacetResult<TResult1, TResult2>(
                names[0],
                names[1],
                (IEnumerable<TResult1>)results[0],
                (IEnumerable<TResult2>)results[1]);
        }

        internal static IBsonSerializer<AggregateFacetResult<TResult1, TResult2>> CreateSerializer(
            IEnumerable<Tuple<string, IBsonSerializer>> facets)
        {
            return new AggregateFacetResultSerializer<AggregateFacetResult<TResult1, TResult2>>(facets, Create);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResult{TResult1, TResult2}"/> class.
        /// </summary>
        public AggregateFacetResult(
            string name1,
            string name2,
            IEnumerable<TResult1> result1,
            IEnumerable<TResult2> result2)
        {
            Name1 = Ensure.IsNotNull(name1, nameof(name1));
            Name2 = Ensure.IsNotNull(name2, nameof(name2));
            Result1 = Ensure.IsNotNull(result1, nameof(name1));
            Result2 = Ensure.IsNotNull(result2, nameof(name2));
        }

        /// <summary>
        /// Gets the name of facet 1.
        /// </summary>
        public string Name1 { get; private set; }

        /// <summary>
        /// Gets the name of facet 2.
        /// </summary>
        public string Name2 { get; private set; }
        /// <summary>
        /// Gets the result of facet 1.
        /// </summary>
        public IEnumerable<TResult1> Result1 { get; private set; }

        /// <summary>
        /// Gets the result of facet 2.
        /// </summary>
        public IEnumerable<TResult2> Result2 { get; private set; }

    }

    /// <summary>
    /// Represents the result of a $facet stage with three facets.
    /// </summary>
    /// <typeparam name="TResult1">The result type of facet 1.</typeparam>
    /// <typeparam name="TResult2">The result type of facet 2.</typeparam>
    /// <typeparam name="TResult3">The result type of facet 3.</typeparam>
    public class AggregateFacetResult<TResult1, TResult2, TResult3>
    {
        internal static AggregateFacetResult<TResult1, TResult2, TResult3> Create(string[] names, object[] results)
        {
            return new AggregateFacetResult<TResult1, TResult2, TResult3>(
                names[0],
                names[1],
                names[2],
                (IEnumerable<TResult1>)results[0],
                (IEnumerable<TResult2>)results[1],
                (IEnumerable<TResult3>)results[2]);
        }

        internal static IBsonSerializer<AggregateFacetResult<TResult1, TResult2, TResult3>> CreateSerializer(
            IEnumerable<Tuple<string, IBsonSerializer>> facets)
        {
            return new AggregateFacetResultSerializer<AggregateFacetResult<TResult1, TResult2, TResult3>>(facets, Create);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResult{TResult1, TResult2, TResult3}"/> class.
        /// </summary>
        public AggregateFacetResult(
            string name1,
            string name2,
            string name3,
            IEnumerable<TResult1> result1,
            IEnumerable<TResult2> result2,
            IEnumerable<TResult3> result3)
        {
            Name1 = Ensure.IsNotNull(name1, nameof(name1));
            Name2 = Ensure.IsNotNull(name2, nameof(name2));
            Name3 = Ensure.IsNotNull(name3, nameof(name3));
            Result1 = Ensure.IsNotNull(result1, nameof(name1));
            Result2 = Ensure.IsNotNull(result2, nameof(name2));
            Result3 = Ensure.IsNotNull(result3, nameof(name3));
        }

        /// <summary>
        /// Gets the name of facet 1.
        /// </summary>
        public string Name1 { get; private set; }

        /// <summary>
        /// Gets the name of facet 2.
        /// </summary>
        public string Name2 { get; private set; }

        /// <summary>
        /// Gets the name of facet 3.
        /// </summary>
        public string Name3 { get; private set; }

        /// <summary>
        /// Gets the result of facet 1.
        /// </summary>
        public IEnumerable<TResult1> Result1 { get; private set; }

        /// <summary>
        /// Gets the result of facet 2.
        /// </summary>
        public IEnumerable<TResult2> Result2 { get; private set; }

        /// <summary>
        /// Gets the result of facet 3.
        /// </summary>
        public IEnumerable<TResult3> Result3 { get; private set; }
    }

    /// <summary>
    /// Represents the result of a $facet stage with four facets.
    /// </summary>
    /// <typeparam name="TResult1">The result type of facet 1.</typeparam>
    /// <typeparam name="TResult2">The result type of facet 2.</typeparam>
    /// <typeparam name="TResult3">The result type of facet 3.</typeparam>
    /// <typeparam name="TResult4">The result type of facet 4.</typeparam>
    public class AggregateFacetResult<TResult1, TResult2, TResult3, TResult4>
    {
        internal static AggregateFacetResult<TResult1, TResult2, TResult3, TResult4> Create(string[] names, object[] results)
        {
            return new AggregateFacetResult<TResult1, TResult2, TResult3, TResult4>(
                names[0],
                names[1],
                names[2],
                names[3],
                (IEnumerable<TResult1>)results[0],
                (IEnumerable<TResult2>)results[1],
                (IEnumerable<TResult3>)results[2],
                (IEnumerable<TResult4>)results[3]);
        }

        internal static IBsonSerializer<AggregateFacetResult<TResult1, TResult2, TResult3, TResult4>> CreateSerializer(
            IEnumerable<Tuple<string, IBsonSerializer>> facets)
        {
            return new AggregateFacetResultSerializer<AggregateFacetResult<TResult1, TResult2, TResult3, TResult4>>(facets, Create);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResult{TResult1, TResult2, TResult3, TResult4}"/> class.
        /// </summary>
        public AggregateFacetResult(
            string name1,
            string name2,
            string name3,
            string name4,
            IEnumerable<TResult1> result1,
            IEnumerable<TResult2> result2,
            IEnumerable<TResult3> result3,
            IEnumerable<TResult4> result4)
        {
            Name1 = Ensure.IsNotNull(name1, nameof(name1));
            Name2 = Ensure.IsNotNull(name2, nameof(name2));
            Name3 = Ensure.IsNotNull(name3, nameof(name3));
            Name4 = Ensure.IsNotNull(name4, nameof(name4));
            Result1 = Ensure.IsNotNull(result1, nameof(name1));
            Result2 = Ensure.IsNotNull(result2, nameof(name2));
            Result3 = Ensure.IsNotNull(result3, nameof(name3));
            Result4 = Ensure.IsNotNull(result4, nameof(name4));
        }

        /// <summary>
        /// Gets the name of facet 1.
        /// </summary>
        public string Name1 { get; private set; }

        /// <summary>
        /// Gets the name of facet 2.
        /// </summary>
        public string Name2 { get; private set; }

        /// <summary>
        /// Gets the name of facet 3.
        /// </summary>
        public string Name3 { get; private set; }

        /// <summary>
        /// Gets the name of facet 4.
        /// </summary>
        public string Name4 { get; private set; }

        /// <summary>
        /// Gets the result of facet 1.
        /// </summary>
        public IEnumerable<TResult1> Result1 { get; private set; }

        /// <summary>
        /// Gets the result of facet 2.
        /// </summary>
        public IEnumerable<TResult2> Result2 { get; private set; }

        /// <summary>
        /// Gets the result of facet 3.
        /// </summary>
        public IEnumerable<TResult3> Result3 { get; private set; }

        /// <summary>
        /// Gets the result of facet 4.
        /// </summary>
        public IEnumerable<TResult4> Result4 { get; private set; }
    }

    internal class AggregateFacetResultSerializer<TAggregateFacetResult> : SerializerBase<TAggregateFacetResult>
    {
        private readonly Func<string[], object[], TAggregateFacetResult> _creator;
        private readonly string[] _names;
        private readonly IBsonSerializer[] _serializers;

        public AggregateFacetResultSerializer(
            IEnumerable<Tuple<string, IBsonSerializer>> facets,
            Func<string[], object[], TAggregateFacetResult> creator)
        {
            _names = facets.Select(f => f.Item1).ToArray();
            _serializers = facets.Select(f => f.Item2).ToArray();
            _creator = creator;
        }

        public override TAggregateFacetResult Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            var results = new object[_names.Length];

            var reader = context.Reader;
            reader.ReadStartDocument();
            while (reader.ReadBsonType() != 0)
            {
                var name = reader.ReadName();
                var index = Array.IndexOf(_names, name);
                if (index != -1)
                {
                    var itemSerializer = _serializers[index];
                    var itemType = itemSerializer.ValueType;
                    var itemSerializerType = typeof(IBsonSerializer<>).MakeGenericType(itemType);
                    var listType = typeof(List<>).MakeGenericType(itemType);
                    var listSerializerType = typeof(EnumerableInterfaceImplementerSerializer<,>).MakeGenericType(listType, itemType);
                    var listSerializerConstructor = listSerializerType.GetTypeInfo().GetConstructor(new[] { itemSerializerType });
                    var listSerializer = (IBsonSerializer)listSerializerConstructor.Invoke(new object[] { itemSerializer });
                    var value = listSerializer.Deserialize(context);
                    results[index] = value;
                }
                else
                {
                    throw new BsonSerializationException($"Unexpected field name '{name}' in $facet result.");
                }
            }
            reader.ReadEndDocument();

            return _creator(_names, results);
        }
    }
}
