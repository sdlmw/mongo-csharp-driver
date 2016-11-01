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

namespace MongoDB.Driver
{
    /// <summary>
    /// Represents the result of a $facet stage with one facet.
    /// </summary>
    /// <typeparam name="TResult1">The result type of facet 1.</typeparam>
    public class AggregateFacetResult<TResult1>
    {
        internal static IBsonSerializer<AggregateFacetResult<TResult1>> CreateSerializer(
            IEnumerable<Tuple<string, IBsonSerializer>> facets)
        {
            var materializedFacets = facets.ToList();
            var setters = new Action<AggregateFacetResult<TResult1>, object>[]
            {
                (r, v) => r.Result1 = (IEnumerable<TResult1>)v
            };
            return new AggregateFacetResultSerializer<AggregateFacetResult<TResult1>>(
                () => new AggregateFacetResult<TResult1>(materializedFacets.Select(f => f.Item1)),
                materializedFacets,
                setters);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResult{TResult1}"/> class.
        /// </summary>
        /// <param name="names">The names.</param>
        public AggregateFacetResult(IEnumerable<string> names)
        {
            var materializedNames = names.ToList();
            Name1 = materializedNames[0];
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
        internal static IBsonSerializer<AggregateFacetResult<TResult1, TResult2>> CreateSerializer(
            IEnumerable<Tuple<string, IBsonSerializer>> facets)
        {
            var materializedFacets = facets.ToList();
            var setters = new Action<AggregateFacetResult<TResult1, TResult2>, object>[]
            {
                (r, v) => r.Result1 = (IEnumerable<TResult1>)v,
                (r, v) => r.Result2 = (IEnumerable<TResult2>)v
            };
            return new AggregateFacetResultSerializer<AggregateFacetResult<TResult1, TResult2>>(
                () => new AggregateFacetResult<TResult1, TResult2>(materializedFacets.Select(f => f.Item1)),
                materializedFacets,
                setters);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResult{TResult1, TResult2, TResult3}"/> class.
        /// </summary>
        /// <param name="names">The names.</param>
        public AggregateFacetResult(IEnumerable<string> names)
        {
            var materializedNames = names.ToList();
            Name1 = materializedNames[0];
            Name2 = materializedNames[1];
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
        internal static IBsonSerializer<AggregateFacetResult<TResult1, TResult2, TResult3>> CreateSerializer(
            IEnumerable<Tuple<string, IBsonSerializer>> facets)
        {
            var materializedFacets = facets.ToList();
            var setters = new Action<AggregateFacetResult<TResult1, TResult2, TResult3>, object>[]
            {
                (r, v) => r.Result1 = (IEnumerable<TResult1>)v,
                (r, v) => r.Result2 = (IEnumerable<TResult2>)v,
                (r, v) => r.Result3 = (IEnumerable<TResult3>)v
            };
            return new AggregateFacetResultSerializer<AggregateFacetResult<TResult1, TResult2, TResult3>>(
                () => new AggregateFacetResult<TResult1, TResult2, TResult3>(materializedFacets.Select(f => f.Item1)),
                materializedFacets,
                setters);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResult{TResult1, TResult2, TResult3}"/> class.
        /// </summary>
        /// <param name="names">The names.</param>
        public AggregateFacetResult(IEnumerable<string> names)
        {
            var materializedNames = names.ToList();
            Name1 = materializedNames[0];
            Name2 = materializedNames[1];
            Name3 = materializedNames[2];
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
        internal static IBsonSerializer<AggregateFacetResult<TResult1, TResult2, TResult3, TResult4>> CreateSerializer(
            IEnumerable<Tuple<string, IBsonSerializer>> facets)
        {
            var materializedFacets = facets.ToList();
            var setters = new Action<AggregateFacetResult<TResult1, TResult2, TResult3, TResult4>, object>[]
            {
                (r, v) => r.Result1 = (IEnumerable<TResult1>)v,
                (r, v) => r.Result2 = (IEnumerable<TResult2>)v,
                (r, v) => r.Result3 = (IEnumerable<TResult3>)v,
                (r, v) => r.Result4 = (IEnumerable<TResult4>)v
            };
            return new AggregateFacetResultSerializer<AggregateFacetResult<TResult1, TResult2, TResult3, TResult4>>(
                () => new AggregateFacetResult<TResult1, TResult2, TResult3, TResult4>(materializedFacets.Select(f => f.Item1)),
                materializedFacets,
                setters);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResult{TResult1, TResult2, TResult3}"/> class.
        /// </summary>
        /// <param name="names">The names.</param>
        public AggregateFacetResult(IEnumerable<string> names)
        {
            var materializedNames = names.ToList();
            Name1 = materializedNames[0];
            Name2 = materializedNames[1];
            Name3 = materializedNames[2];
            Name4 = materializedNames[3];
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
        private readonly Func<TAggregateFacetResult> _creator;
        private readonly List<Tuple<string, IBsonSerializer, Action<TAggregateFacetResult, object>>> _facets;

        public AggregateFacetResultSerializer(
            Func<TAggregateFacetResult> creator,
            IEnumerable<Tuple<string, IBsonSerializer>> facets,
            IEnumerable<Action<TAggregateFacetResult, object>> setters)
        {
            _creator = creator;
            _facets = facets.Zip(setters, (f, s) => Tuple.Create(f.Item1, f.Item2, s)).ToList();
        }

        public override TAggregateFacetResult Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            var result = _creator();

            var reader = context.Reader;
            reader.ReadStartDocument();
            while (reader.ReadBsonType() != 0)
            {
                var name = reader.ReadName();
                var facet = _facets.Where(s => s.Item1 == name).SingleOrDefault();
                if (facet != null)
                {
                    var itemSerializer = facet.Item2;
                    var itemType = itemSerializer.ValueType;
                    var itemSerializerType = typeof(IBsonSerializer<>).MakeGenericType(itemType);
                    var listType = typeof(List<>).MakeGenericType(itemType);
                    var listSerializerType = typeof(EnumerableInterfaceImplementerSerializer<,>).MakeGenericType(listType, itemType);
                    var listSerializerConstructor = listSerializerType.GetTypeInfo().GetConstructor(new[] { itemSerializerType });
                    var listSerializer = (IBsonSerializer)listSerializerConstructor.Invoke(new object[] { itemSerializer });
                    var value = listSerializer.Deserialize(context);
                    facet.Item3(result, value);
                }
                else
                {
                    throw new BsonSerializationException($"Unexpected field name '{name}' in $facet result.");
                }
            }
            reader.ReadEndDocument();

            return result;
        }
    }
}
