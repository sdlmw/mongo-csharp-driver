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
    /// Represents the results of a $facet stage with an arbitrary number of facets.
    /// </summary>
    public class AggregateFacetResults
    {
        internal static IBsonSerializer<AggregateFacetResults> CreateSerializer(
            IEnumerable<Tuple<string, IBsonSerializer>> facets)
        {
            var facetResultCreators = facets
                .Select(facet =>
                {
                    var outputSerializer = facet.Item2;
                    var outputType = outputSerializer.ValueType;
                    var facetResultCreatorMethodInfo = typeof(AggregateFacetResults).GetTypeInfo().GetMethod("FacetResultCreator", BindingFlags.NonPublic | BindingFlags.Static).MakeGenericMethod(outputType);
                    return (Func<string, Array, AggregateFacetResult>)facetResultCreatorMethodInfo.CreateDelegate(typeof(Func<string, Array, AggregateFacetResult>));
                })
                .ToArray();

            return new AggregateFacetsResultSerializer<AggregateFacetResults>(
                facets,
                facetResults => new AggregateFacetResults(facetResults),
                facetResultCreators);
        }

        private static AggregateFacetResult FacetResultCreator<TOutput>(string name, Array output)
        {
            return new AggregateFacetResult<TOutput>(name, (TOutput[])output);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResults"/> class.
        /// </summary>
        /// <param name="facets">The facets.</param>
        public AggregateFacetResults(AggregateFacetResult[] facets)
        {
            Facets = Ensure.IsNotNull(facets, nameof(facets));
        }

        /// <summary>
        /// Gets the facets.
        /// </summary>
        public IReadOnlyList<AggregateFacetResult> Facets { get; private set; }

        /// <summary>
        /// Gets a facet.
        /// </summary>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="index">The index.</param>
        /// <returns>The facet.</returns>
        public AggregateFacetResult<TOutput> GetFacet<TOutput>(int index)
        {
            return (AggregateFacetResult<TOutput>)Facets[index];
        }
    }

    /// <summary>
    /// Represents the result of a $facet stage with one facet.
    /// </summary>
    /// <typeparam name="TOutput1">The output type of facet 1.</typeparam>
    public class AggregateFacetResults<TOutput1> : AggregateFacetResults
    {
        internal new static IBsonSerializer<AggregateFacetResults<TOutput1>> CreateSerializer(
            IEnumerable<Tuple<string, IBsonSerializer>> facets)
        {
            return new AggregateFacetsResultSerializer<AggregateFacetResults<TOutput1>>(
                facets,
                facetResults => new AggregateFacetResults<TOutput1>(facetResults),
                (name, output) => new AggregateFacetResult<TOutput1>(name, (TOutput1[])output));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResults{TOutput1}"/> class.
        /// </summary>
        public AggregateFacetResults(AggregateFacetResult[] facets)
            : base(facets)
        {
        }

        /// <summary>
        /// Gets facet 1.
        /// </summary>
        public AggregateFacetResult<TOutput1> Facet1 => (AggregateFacetResult<TOutput1>)Facets[0];
    }

    /// <summary>
    /// Represents the result of a $facet stage with two facets.
    /// </summary>
    /// <typeparam name="TOutput1">The output type of facet 1.</typeparam>
    /// <typeparam name="TOutput2">The output type of facet 2.</typeparam>
    public class AggregateFacetResults<TOutput1, TOutput2> : AggregateFacetResults
    {
        internal new static IBsonSerializer<AggregateFacetResults<TOutput1, TOutput2>> CreateSerializer(
            IEnumerable<Tuple<string, IBsonSerializer>> facets)
        {
            return new AggregateFacetsResultSerializer<AggregateFacetResults<TOutput1, TOutput2>>(
                facets,
                facetResults => new AggregateFacetResults<TOutput1, TOutput2>(facetResults),
                (name, output) => new AggregateFacetResult<TOutput1>(name, (TOutput1[])output),
                (name, output) => new AggregateFacetResult<TOutput2>(name, (TOutput2[])output));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResults{TOutput1, TOutput2}"/> class.
        /// </summary>
        public AggregateFacetResults(AggregateFacetResult[] facets)
            : base(facets)
        {
        }

        /// <summary>
        /// Gets facet 1.
        /// </summary>
        public AggregateFacetResult<TOutput1> Facet1 => (AggregateFacetResult<TOutput1>)Facets[0];

        /// <summary>
        /// Gets facet 2.
        /// </summary>
        public AggregateFacetResult<TOutput2> Facet2 => (AggregateFacetResult<TOutput2>)Facets[1];
    }

    /// <summary>
    /// Represents the result of a $facet stage with three facets.
    /// </summary>
    /// <typeparam name="TOutput1">The output type of facet 1.</typeparam>
    /// <typeparam name="TOutput2">The output type of facet 2.</typeparam>
    /// <typeparam name="TOutput3">The output type of facet 3.</typeparam>
    public class AggregateFacetResults<TOutput1, TOutput2, TOutput3> : AggregateFacetResults
    {
        internal new static IBsonSerializer<AggregateFacetResults<TOutput1, TOutput2, TOutput3>> CreateSerializer(
            IEnumerable<Tuple<string, IBsonSerializer>> facets)
        {
            return new AggregateFacetsResultSerializer<AggregateFacetResults<TOutput1, TOutput2, TOutput3>>(
                facets,
                facetResults => new AggregateFacetResults<TOutput1, TOutput2, TOutput3>(facetResults),
                (name, output) => new AggregateFacetResult<TOutput1>(name, (TOutput1[])output),
                (name, output) => new AggregateFacetResult<TOutput2>(name, (TOutput2[])output),
                (name, output) => new AggregateFacetResult<TOutput3>(name, (TOutput3[])output));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResults{TOutput1, TOutput2, TOutput3}"/> class.
        /// </summary>
        public AggregateFacetResults(AggregateFacetResult[] facets)
            : base(facets)
        {
        }

        /// <summary>
        /// Gets facet 1.
        /// </summary>
        public AggregateFacetResult<TOutput1> Facet1 => (AggregateFacetResult<TOutput1>)Facets[0];

        /// <summary>
        /// Gets facet 2.
        /// </summary>
        public AggregateFacetResult<TOutput2> Facet2 => (AggregateFacetResult<TOutput2>)Facets[1];

        /// <summary>
        /// Gets facet 3.
        /// </summary>
        public AggregateFacetResult<TOutput3> Facet3 => (AggregateFacetResult<TOutput3>)Facets[2];
    }

    /// <summary>
    /// Represents the result of a $facet stage with four facets.
    /// </summary>
    /// <typeparam name="TOutput1">The output type of facet 1.</typeparam>
    /// <typeparam name="TOutput2">The output type of facet 2.</typeparam>
    /// <typeparam name="TOutput3">The output type of facet 3.</typeparam>
    /// <typeparam name="TOutput4">The output type of facet 4.</typeparam>
    public class AggregateFacetResults<TOutput1, TOutput2, TOutput3, TOutput4> : AggregateFacetResults
    {
        internal new static IBsonSerializer<AggregateFacetResults<TOutput1, TOutput2, TOutput3, TOutput4>> CreateSerializer(
            IEnumerable<Tuple<string, IBsonSerializer>> facets)
        {
            return new AggregateFacetsResultSerializer<AggregateFacetResults<TOutput1, TOutput2, TOutput3, TOutput4>>(
                facets,
                facetResults => new AggregateFacetResults<TOutput1, TOutput2, TOutput3, TOutput4>(facetResults),
                (name, output) => new AggregateFacetResult<TOutput1>(name, (TOutput1[])output),
                (name, output) => new AggregateFacetResult<TOutput2>(name, (TOutput2[])output),
                (name, output) => new AggregateFacetResult<TOutput3>(name, (TOutput3[])output),
                (name, output) => new AggregateFacetResult<TOutput4>(name, (TOutput4[])output));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResults{TOutput1, TOutput2, TOutput3, TOutput4}"/> class.
        /// </summary>
        public AggregateFacetResults(AggregateFacetResult[] facets)
            : base(facets)
        {
        }

        /// <summary>
        /// Gets facet 1.
        /// </summary>
        public AggregateFacetResult<TOutput1> Facet1 => (AggregateFacetResult<TOutput1>)Facets[0];

        /// <summary>
        /// Gets facet 2.
        /// </summary>
        public AggregateFacetResult<TOutput2> Facet2 => (AggregateFacetResult<TOutput2>)Facets[1];

        /// <summary>
        /// Gets facet 3.
        /// </summary>
        public AggregateFacetResult<TOutput3> Facet3 => (AggregateFacetResult<TOutput3>)Facets[2];

        /// <summary>
        /// Gets facet 4.
        /// </summary>
        public AggregateFacetResult<TOutput4> Facet4 => (AggregateFacetResult<TOutput4>)Facets[3];
    }

    internal class AggregateFacetsResultSerializer<TAggregateFacetsResult> : SerializerBase<TAggregateFacetsResult>
    {
        private readonly Func<AggregateFacetResult[], TAggregateFacetsResult> _creator;
        private readonly Func<string, Array, AggregateFacetResult>[] _facetResultCreators;
        private readonly string[] _names;
        private readonly IBsonSerializer[] _serializers;

        public AggregateFacetsResultSerializer(
            IEnumerable<Tuple<string, IBsonSerializer>> facets,
            Func<AggregateFacetResult[], TAggregateFacetsResult> creator,
            params Func<string, Array, AggregateFacetResult>[] facetResultCreators)
        {
            _names = facets.Select(f => f.Item1).ToArray();
            _serializers = facets.Select(f => f.Item2).ToArray();
            _creator = creator;
            _facetResultCreators = facetResultCreators;
        }

        public override TAggregateFacetsResult Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            var facetResults = new AggregateFacetResult[_names.Length];

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
                    var arraySerializerType = typeof(ArraySerializer<>).MakeGenericType(itemType);
                    var arraySerializerConstructor = arraySerializerType.GetTypeInfo().GetConstructor(new[] { itemSerializerType });
                    var arraySerializer = (IBsonSerializer)arraySerializerConstructor.Invoke(new object[] { itemSerializer });
                    var output = (Array)arraySerializer.Deserialize(context);
                    var facetResult = _facetResultCreators[index](name, output);
                    facetResults[index] = facetResult;
                }
                else
                {
                    throw new BsonSerializationException($"Unexpected field name '{name}' in $facet result.");
                }
            }
            reader.ReadEndDocument();

            var missingIndex = Array.IndexOf(facetResults, null);
            if (missingIndex != -1)
            {
                var missingName = _names[missingIndex];
                throw new BsonSerializationException($"Field name '{missingName}' in $facet result.");
            }


            return _creator(facetResults);
        }
    }
}
