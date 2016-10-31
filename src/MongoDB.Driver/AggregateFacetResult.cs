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

using System.Collections.Generic;
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
        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResult{TResult1}"/> class.
        /// </summary>
        /// <param name="name1">The name of facet 1.</param>
        /// <param name="result1">The result of facet 1.</param>
        public AggregateFacetResult(string name1, IEnumerable<TResult1> result1)
        {
            Name1 = name1;
            Result1 = result1;
        }

        /// <summary>
        /// Gets the name of facet 1.
        /// </summary>
        public string Name1 { get; private set; }

        /// <summary>
        /// Gets the result of facet 1.
        /// </summary>
        public IEnumerable<TResult1> Result1 { get; private set; }

        // nested types
        internal class Serializer : SerializerBase<AggregateFacetResult<TResult1>>
        {
            private readonly string _name1;
            private readonly IBsonSerializer<List<TResult1>> _result1Serializer;

            public Serializer(string name1, IBsonSerializer<TResult1> result1Serializer)
            {
                _name1 = name1;
                _result1Serializer = new EnumerableInterfaceImplementerSerializer<List<TResult1>, TResult1>(result1Serializer);
            }

            public override AggregateFacetResult<TResult1> Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
            {
                List<TResult1> result1 = new List<TResult1>();

                var reader = context.Reader;
                reader.ReadStartDocument();
                while (reader.ReadBsonType() != 0)
                {
                    var name = reader.ReadName();
                    if (name == _name1)
                    {
                        result1 = _result1Serializer.Deserialize(context);
                    }
                    else
                    {
                        throw new BsonSerializationException($"Unexpected field name '{name}' in $facet result.");
                    }
                }
                reader.ReadEndDocument();

                return new AggregateFacetResult<TResult1>(_name1, result1);
            }
        }
    }

    /// <summary>
    /// Represents the result of a $facet stage with two facets.
    /// </summary>
    /// <typeparam name="TResult1">The result type of facet 1.</typeparam>
    /// <typeparam name="TResult2">The result type of facet 2.</typeparam>
    public class AggregateFacetResult<TResult1, TResult2>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResult{TResult1, TResult2}"/> class.
        /// </summary>
        /// <param name="name1">The name of facet 1.</param>
        /// <param name="result1">The result of facet 1.</param>
        /// <param name="name2">The name of facet 2.</param>
        /// <param name="result2">The result of facet 2.</param>
        public AggregateFacetResult(
            string name1,
            IEnumerable<TResult1> result1,
            string name2,
            IEnumerable<TResult2> result2)
        {
            Name1 = name1;
            Result1 = result1;
            Name2 = name2;
            Result2 = result2;
        }

        /// <summary>
        /// Gets the name of facet 1.
        /// </summary>
        public string Name1 { get; set; }

        /// <summary>
        /// Gets the name of facet 2.
        /// </summary>
        public string Name2 { get; set; }

        /// <summary>
        /// Gets the result of facet 1.
        /// </summary>
        public IEnumerable<TResult1> Result1 { get; set; }

        /// <summary>
        /// Gets the result of facet 2.
        /// </summary>
        public IEnumerable<TResult2> Result2 { get; set; }

        // nested types
        internal class Serializer : SerializerBase<AggregateFacetResult<TResult1, TResult2>>
        {
            private readonly string _name1;
            private readonly string _name2;
            private readonly IBsonSerializer<List<TResult1>> _result1Serializer;
            private readonly IBsonSerializer<List<TResult2>> _result2Serializer;

            public Serializer(
                string name1,
                IBsonSerializer<TResult1> result1Serializer,
                string name2,
                IBsonSerializer<TResult2> result2Serializer)
            {
                _name1 = name1;
                _result1Serializer = new EnumerableInterfaceImplementerSerializer<List<TResult1>, TResult1>(result1Serializer);
                _name2 = name2;
                _result2Serializer = new EnumerableInterfaceImplementerSerializer<List<TResult2>, TResult2>(result2Serializer);
            }

            public override AggregateFacetResult<TResult1, TResult2> Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
            {
                List<TResult1> result1 = new List<TResult1>();
                List<TResult2> result2 = new List<TResult2>();

                var reader = context.Reader;
                reader.ReadStartDocument();
                while (reader.ReadBsonType() != 0)
                {
                    var name = reader.ReadName();
                    if (name == _name1)
                    {
                        result1 = _result1Serializer.Deserialize(context);
                    }
                    else if (name == _name2)
                    {
                        result2 = _result2Serializer.Deserialize(context);
                    }
                    else
                    {
                        throw new BsonSerializationException($"Unexpected field name '{name}' in $facet result.");
                    }
                }
                reader.ReadEndDocument();

                return new AggregateFacetResult<TResult1, TResult2>(
                    _name1,
                    result1,
                    _name2,
                    result2);
            }
        }
    }

    /// <summary>
    /// Represents the result of a $facet stage with three facets.
    /// </summary>
    /// <typeparam name="TResult1">The result type of facet 1.</typeparam>
    /// <typeparam name="TResult2">The result type of facet 2.</typeparam>
    /// <typeparam name="TResult3">The result type of facet 3.</typeparam>
    public class AggregateFacetResult<TResult1, TResult2, TResult3>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacetResult{TResult1, TResult2, TResult3}"/> class.
        /// </summary>
        /// <param name="name1">The name of facet 1.</param>
        /// <param name="result1">The result of facet 1.</param>
        /// <param name="name2">The name of facet 2.</param>
        /// <param name="result2">The result of facet 2.</param>
        /// <param name="name3">The name of facet 3.</param>
        /// <param name="result3">The result of facet 3.</param>
        public AggregateFacetResult(
            string name1,
            IEnumerable<TResult1> result1,
            string name2,
            IEnumerable<TResult2> result2,
            string name3,
            IEnumerable<TResult3> result3)
        {
            Name1 = name1;
            Result1 = result1;
            Name2 = name2;
            Result2 = result2;
            Name3 = name3;
            Result3 = result3;
        }

        /// <summary>
        /// Gets the name of facet 1.
        /// </summary>
        public string Name1 { get; set; }

        /// <summary>
        /// Gets the name of facet 2.
        /// </summary>
        public string Name2 { get; set; }

        /// <summary>
        /// Gets the name of facet 3.
        /// </summary>
        public string Name3 { get; set; }

        /// <summary>
        /// Gets the result of facet 1.
        /// </summary>
        public IEnumerable<TResult1> Result1 { get; set; }

        /// <summary>
        /// Gets the result of facet 2.
        /// </summary>
        public IEnumerable<TResult2> Result2 { get; set; }

        /// <summary>
        /// Gets the result of facet 3.
        /// </summary>
        public IEnumerable<TResult3> Result3 { get; set; }

        // nested types
        internal class Serializer : SerializerBase<AggregateFacetResult<TResult1, TResult2, TResult3>>
        {
            private readonly string _name1;
            private readonly string _name2;
            private readonly string _name3;
            private readonly IBsonSerializer<List<TResult1>> _result1Serializer;
            private readonly IBsonSerializer<List<TResult2>> _result2Serializer;
            private readonly IBsonSerializer<List<TResult3>> _result3Serializer;

            public Serializer(
                string name1,
                IBsonSerializer<TResult1> result1Serializer,
                string name2,
                IBsonSerializer<TResult2> result2Serializer,
                string name3,
                IBsonSerializer<TResult3> result3Serializer)
            {
                _name1 = name1;
                _result1Serializer = new EnumerableInterfaceImplementerSerializer<List<TResult1>, TResult1>(result1Serializer);
                _name2 = name2;
                _result2Serializer = new EnumerableInterfaceImplementerSerializer<List<TResult2>, TResult2>(result2Serializer);
                _name3 = name3;
                _result3Serializer = new EnumerableInterfaceImplementerSerializer<List<TResult3>, TResult3>(result3Serializer);
            }

            public override AggregateFacetResult<TResult1, TResult2, TResult3> Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
            {
                List<TResult1> result1 = new List<TResult1>();
                List<TResult2> result2 = new List<TResult2>();
                List<TResult3> result3 = new List<TResult3>();

                var reader = context.Reader;
                reader.ReadStartDocument();
                while (reader.ReadBsonType() != 0)
                {
                    var name = reader.ReadName();
                    if (name == _name1)
                    {
                        result1 = _result1Serializer.Deserialize(context);
                    }
                    else if (name == _name2)
                    {
                        result2 = _result2Serializer.Deserialize(context);
                    }
                    else if (name == _name3)
                    {
                        result3 = _result3Serializer.Deserialize(context);
                    }
                    else
                    {
                        throw new BsonSerializationException($"Unexpected field name '{name}' in $facet result.");
                    }
                }
                reader.ReadEndDocument();

                return new AggregateFacetResult<TResult1, TResult2, TResult3>(
                    _name1,
                    result1,
                    _name2,
                    result2,
                    _name3,
                    result3);
            }
        }
    }
}
