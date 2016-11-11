/* Copyright 2010-2016 MongoDB Inc.
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
using System.Linq.Expressions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver.Core.Misc;
using MongoDB.Driver.Linq.Translators;

namespace MongoDB.Driver
{
    /// <summary>
    /// Methods for building pipeline stages.
    /// </summary>
    public static class PipelineStageDefinitionBuilder
    {
        /// <summary>
        /// Creates a $bucket stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="boundaries">The boundaries.</param>
        /// <param name="options">The options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, AggregateBucketResult<TValue>> Bucket<TInput, TValue>(
            AggregateExpressionDefinition<TInput, TValue> groupBy,
            IEnumerable<TValue> boundaries,
            AggregateBucketOptions<TValue> options = null)
        {
            Ensure.IsNotNull(groupBy, nameof(groupBy));
            Ensure.IsNotNull(boundaries, nameof(boundaries));

            const string operatorName = "$bucket";
            var stage = new DelegatedPipelineStageDefinition<TInput, AggregateBucketResult<TValue>>(
                operatorName,
                (s, sr) =>
                {
                    var valueSerializer = sr.GetSerializer<TValue>();
                    var renderedGroupBy = groupBy.Render(s, sr);
                    var serializedBoundaries = boundaries.Select(b => valueSerializer.ToBsonValue(b));
                    var serializedDefaultBucket = options != null && options.DefaultBucket.HasValue ? valueSerializer.ToBsonValue(options.DefaultBucket.Value) : null;
                    var document = new BsonDocument
                    {
                        { operatorName, new BsonDocument
                            {
                                { "groupBy", renderedGroupBy },
                                { "boundaries", new BsonArray(serializedBoundaries) },
                                { "default", serializedDefaultBucket, serializedDefaultBucket != null }
                            }
                        }
                    };
                    return new RenderedPipelineStageDefinition<AggregateBucketResult<TValue>>(
                        operatorName,
                        document,
                        sr.GetSerializer<AggregateBucketResult<TValue>>());
                });

            return stage;
        }

        /// <summary>
        /// Creates a $bucket stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="boundaries">The boundaries.</param>
        /// <param name="output">The output projection.</param>
        /// <param name="options">The options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> Bucket<TInput, TValue, TOutput>(
            AggregateExpressionDefinition<TInput, TValue> groupBy,
            IEnumerable<TValue> boundaries,
            ProjectionDefinition<TInput, TOutput> output,
            AggregateBucketOptions<TValue> options = null)
        {
            Ensure.IsNotNull(groupBy, nameof(groupBy));
            Ensure.IsNotNull(boundaries, nameof(boundaries));
            Ensure.IsNotNull(output, nameof(output));

            const string operatorName = "$bucket";
            var stage = new DelegatedPipelineStageDefinition<TInput, TOutput>(
                operatorName,
                (s, sr) =>
                {
                    var valueSerializer = sr.GetSerializer<TValue>();
                    var newResultSerializer = sr.GetSerializer<TOutput>();
                    var renderedGroupBy = groupBy.Render(s, sr);
                    var serializedBoundaries = boundaries.Select(b => valueSerializer.ToBsonValue(b));
                    var serializedDefaultBucket = options != null && options.DefaultBucket.HasValue ? valueSerializer.ToBsonValue(options.DefaultBucket.Value) : null;
                    var renderedOutput = output.Render(s, sr);
                    var document = new BsonDocument
                    {
                        { operatorName, new BsonDocument
                            {
                                { "groupBy", renderedGroupBy },
                                { "boundaries", new BsonArray(serializedBoundaries) },
                                { "default", serializedDefaultBucket, serializedDefaultBucket != null },
                                { "output", renderedOutput.Document }
                            }
                        }
                    };
                    return new RenderedPipelineStageDefinition<TOutput>(
                        operatorName,
                        document,
                        newResultSerializer);
                });

            return stage;
        }

        /// <summary>
        /// Creates a $bucket stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="boundaries">The boundaries.</param>
        /// <param name="options">The options.</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, AggregateBucketResult<TValue>> Bucket<TInput, TValue>(
            Expression<Func<TInput, TValue>> groupBy,
            IEnumerable<TValue> boundaries,
            AggregateBucketOptions<TValue> options = null,
            ExpressionTranslationOptions translationOptions = null)
        {
            Ensure.IsNotNull(groupBy, nameof(groupBy));
            Ensure.IsNotNull(boundaries, nameof(boundaries));

            var groupByDefinition = new ExpressionAggregateExpressionDefinition<TInput, TValue>(groupBy, translationOptions);
            return Bucket(groupByDefinition, boundaries, options);
        }

        /// <summary>
        /// Creates a $bucket stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="boundaries">The boundaries.</param>
        /// <param name="output">The output projection.</param>
        /// <param name="options">The options.</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> Bucket<TInput, TValue, TOutput>(
            Expression<Func<TInput, TValue>> groupBy,
            IEnumerable<TValue> boundaries,
            Expression<Func<IGrouping<TValue, TInput>, TOutput>> output,
            AggregateBucketOptions<TValue> options = null,
            ExpressionTranslationOptions translationOptions = null)
        {
            Ensure.IsNotNull(groupBy, nameof(groupBy));
            Ensure.IsNotNull(boundaries, nameof(boundaries));
            Ensure.IsNotNull(output, nameof(output));

            var groupByDefinition = new ExpressionAggregateExpressionDefinition<TInput, TValue>(groupBy, translationOptions);
            var outputDefinition = new ExpressionBucketOutputProjection<TInput, TValue, TOutput>(x => default(TValue), output, translationOptions);
            return Bucket(groupByDefinition, boundaries, outputDefinition, options);
        }

        /// <summary>
        /// Creates a $bucketAuto stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="buckets">The number of buckets.</param>
        /// <param name="options">The options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, AggregateBucketAutoResult<TValue>> BucketAuto<TInput, TValue>(
            AggregateExpressionDefinition<TInput, TValue> groupBy,
            int buckets,
            AggregateBucketAutoOptions options = null)
        {
            Ensure.IsNotNull(groupBy, nameof(groupBy));
            Ensure.IsGreaterThanZero(buckets, nameof(buckets));

            const string operatorName = "$bucketAuto";
            var stage = new DelegatedPipelineStageDefinition<TInput, AggregateBucketAutoResult<TValue>>(
                operatorName,
                (s, sr) =>
                {
                    var renderedGroupBy = groupBy.Render(s, sr);
                    var document = new BsonDocument
                    {
                            { operatorName, new BsonDocument
                                {
                                    { "groupBy", renderedGroupBy },
                                    { "buckets", buckets },
                                    { "granularity", () => options.Granularity.Value.Value, options != null && options.Granularity.HasValue }
                                }
                            }
                    };
                    return new RenderedPipelineStageDefinition<AggregateBucketAutoResult<TValue>>(
                        operatorName,
                        document,
                        sr.GetSerializer<AggregateBucketAutoResult<TValue>>());
                });

            return stage;
        }

        /// <summary>
        /// Creates a $bucketAuto stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="buckets">The number of buckets.</param>
        /// <param name="output">The output projection.</param>
        /// <param name="options">The options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> BucketAuto<TInput, TValue, TOutput>(
            AggregateExpressionDefinition<TInput, TValue> groupBy,
            int buckets,
            ProjectionDefinition<TInput, TOutput> output,
            AggregateBucketAutoOptions options = null)
        {
            Ensure.IsNotNull(groupBy, nameof(groupBy));
            Ensure.IsGreaterThanZero(buckets, nameof(buckets));
            Ensure.IsNotNull(output, nameof(output));

            const string operatorName = "$bucketAuto";
            var stage = new DelegatedPipelineStageDefinition<TInput, TOutput>(
                operatorName,
                (s, sr) =>
                {
                    var newResultSerializer = sr.GetSerializer<TOutput>();
                    var renderedGroupBy = groupBy.Render(s, sr);
                    var renderedOutput = output.Render(s, sr);
                    var document = new BsonDocument
                    {
                        { operatorName, new BsonDocument
                            {
                                { "groupBy", renderedGroupBy },
                                { "buckets", buckets },
                                { "output", renderedOutput.Document },
                                { "granularity", () => options.Granularity.Value.Value, options != null && options.Granularity.HasValue }
                           }
                        }
                    };
                    return new RenderedPipelineStageDefinition<TOutput>(
                        operatorName,
                        document,
                        newResultSerializer);
                });

            return stage;
        }

        /// <summary>
        /// Creates a $bucketAuto stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="buckets">The number of buckets.</param>
        /// <param name="options">The options (optional).</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, AggregateBucketAutoResult<TValue>> BucketAuto<TInput, TValue>(
            Expression<Func<TInput, TValue>> groupBy,
            int buckets,
            AggregateBucketAutoOptions options = null,
            ExpressionTranslationOptions translationOptions = null)
        {
            Ensure.IsNotNull(groupBy, nameof(groupBy));

            var groupByDefinition = new ExpressionAggregateExpressionDefinition<TInput, TValue>(groupBy, translationOptions);
            return BucketAuto(groupByDefinition, buckets, options);
        }

        /// <summary>
        /// Creates a $bucketAuto stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TValue">The type of the output documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="buckets">The number of buckets.</param>
        /// <param name="output">The output projection.</param>
        /// <param name="options">The options (optional).</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> BucketAuto<TInput, TValue, TOutput>(
            Expression<Func<TInput, TValue>> groupBy,
            int buckets,
            Expression<Func<IGrouping<TValue, TInput>, TOutput>> output,
            AggregateBucketAutoOptions options = null,
            ExpressionTranslationOptions translationOptions = null)
        {
            Ensure.IsNotNull(groupBy, nameof(groupBy));
            Ensure.IsNotNull(output, nameof(output));

            var groupByDefinition = new ExpressionAggregateExpressionDefinition<TInput, TValue>(groupBy, translationOptions);
            var outputDefinition = new ExpressionBucketOutputProjection<TInput, TValue, TOutput>(x => default(TValue), output, translationOptions);
            return BucketAuto(groupByDefinition, buckets, outputDefinition, options);
        }

        /// <summary>
        /// Creates a $count stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, AggregateCountResult> Count<TInput>()
        {
            const string operatorName = "$count";
            var stage = new DelegatedPipelineStageDefinition<TInput, AggregateCountResult>(
                operatorName,
                (s, sr) =>
                {
                    return new RenderedPipelineStageDefinition<AggregateCountResult>(
                        operatorName,
                        new BsonDocument(operatorName, "count"),
                        sr.GetSerializer<AggregateCountResult>());
                });

            return stage;
        }

        /// <summary>
        /// Creates a $facet stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="facets">The facets.</param>
        /// <param name="options">The options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> Facet<TInput, TOutput>(
            IEnumerable<AggregateFacet<TInput>> facets,
            AggregateFacetOptions<TOutput> options = null)
        {
            const string operatorName = "$facet";
            var materializedFacets = facets.ToArray();
            var stage = new DelegatedPipelineStageDefinition<TInput, TOutput>(
                operatorName,
                (s, sr) =>
                {
                    var facetsDocument = new BsonDocument();
                    foreach (var facet in materializedFacets)
                    {
                        var renderedPipeline = facet.RenderPipeline(s, sr);
                        facetsDocument.Add(facet.Name, renderedPipeline);
                    }
                    var document = new BsonDocument("$facet", facetsDocument);
                    var resultSerializer = options?.NewResultSerializer ?? sr.GetSerializer<TOutput>();
                    return new RenderedPipelineStageDefinition<TOutput>(operatorName, document, resultSerializer);
                });

            return stage;
        }

        /// <summary>
        /// Creates a $facet stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <param name="facets">The facets.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, AggregateFacetResults> Facet<TInput>(
            IEnumerable<AggregateFacet<TInput>> facets)
        {
            var outputSerializer = new AggregateFacetResultsSerializer(
                facets.Select(f => f.Name),
                facets.Select(f => f.OutputSerializer ?? BsonSerializer.SerializerRegistry.GetSerializer(f.OutputType)));
            var options = new AggregateFacetOptions<AggregateFacetResults> { NewResultSerializer = outputSerializer };
            return Facet(facets, options);
        }

        /// <summary>
        /// Creates a $facet stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <param name="facets">The facets.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, AggregateFacetResults> Facet<TInput>(
            params AggregateFacet<TInput>[] facets)
        {
            return Facet((IEnumerable<AggregateFacet<TInput>>)facets);
        }

        /// <summary>
        /// Creates a $facet stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="facets">The facets.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> Facet<TInput, TOutput>(
            params AggregateFacet<TInput>[] facets)
        {
            return Facet<TInput, TOutput>((IEnumerable<AggregateFacet<TInput>>)facets);
        }

        /// <summary>
        /// Creates a $group stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="group">The group projection.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> Group<TInput, TOutput>(
            ProjectionDefinition<TInput, TOutput> group)
        {
            const string operatorName = "$group";
            var stage = new DelegatedPipelineStageDefinition<TInput, TOutput>(
                operatorName,
                (s, sr) =>
                {
                    var renderedProjection = group.Render(s, sr);
                    return new RenderedPipelineStageDefinition<TOutput>(operatorName, new BsonDocument(operatorName, renderedProjection.Document), renderedProjection.ProjectionSerializer);
                });

            return stage;
        }

        /// <summary>
        /// Creates a $group stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <param name="group">The group projection.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, BsonDocument> Group<TInput>(
            ProjectionDefinition<TInput, BsonDocument> group)
        {
            Ensure.IsNotNull(group, nameof(group));

            return Group<TInput, BsonDocument>(group);
        }

        /// <summary>
        /// Creates a $group stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="value">The value field.</param>
        /// <param name="group">The group projection.</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> Group<TInput, TValue, TOutput>(
            Expression<Func<TInput, TValue>> value,
            Expression<Func<IGrouping<TValue, TInput>, TOutput>> group,
            ExpressionTranslationOptions translationOptions = null)
        {
            Ensure.IsNotNull(value, nameof(value));
            Ensure.IsNotNull(group, nameof(group));

            var groupDefinition = new GroupExpressionProjection<TInput, TValue, TOutput>(value, group, translationOptions);
            return Group(groupDefinition);
        }

        /// <summary>
        /// Creates a $limit stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <param name="limit">The limit.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TInput> Limit<TInput>(
            int limit)
        {
            return new BsonDocumentPipelineStageDefinition<TInput, TInput>(new BsonDocument("$limit", limit));
        }

        /// <summary>
        /// Creates a $lookup stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TForeignDocument">The type of the foreign collection documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="foreignCollectionName">The foreign collection.</param>
        /// <param name="localField">The local field.</param>
        /// <param name="foreignField">The foreign field.</param>
        /// <param name="as">The "as" field.</param>
        /// <param name="options">The options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> Lookup<TInput, TForeignDocument, TOutput>(
            string foreignCollectionName,
            FieldDefinition<TInput> localField,
            FieldDefinition<TForeignDocument> foreignField,
            FieldDefinition<TOutput> @as,
            AggregateLookupOptions<TForeignDocument, TOutput> options = null)
        {
            options = options ?? new AggregateLookupOptions<TForeignDocument, TOutput>();
            const string operatorName = "$lookup";
            var stage = new DelegatedPipelineStageDefinition<TInput, TOutput>(
                operatorName,
                (inputSerializer, sr) =>
                {
                    var foreignSerializer = options.ForeignSerializer ?? (inputSerializer as IBsonSerializer<TForeignDocument>) ?? sr.GetSerializer<TForeignDocument>();
                    var outputSerializer = options.ResultSerializer ?? (inputSerializer as IBsonSerializer<TOutput>) ?? sr.GetSerializer<TOutput>();
                    return new RenderedPipelineStageDefinition<TOutput>(
                        operatorName, new BsonDocument(operatorName, new BsonDocument
                        {
                            { "from", foreignCollectionName },
                            { "localField", localField.Render(inputSerializer, sr).FieldName },
                            { "foreignField", foreignField.Render(foreignSerializer, sr).FieldName },
                            { "as", @as.Render(outputSerializer, sr).FieldName }
                        }),
                        outputSerializer);
                });

            return stage;
        }

        /// <summary>
        /// Creates a $lookup stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TForeignDocument">The type of the foreign collection documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="foreignCollectionName">The foreign collection.</param>
        /// <param name="localField">The local field.</param>
        /// <param name="foreignField">The foreign field.</param>
        /// <param name="as">The "as" field.</param>
        /// <param name="options">The options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> Lookup<TInput, TForeignDocument, TOutput>(
            string foreignCollectionName,
            Expression<Func<TInput, object>> localField,
            Expression<Func<TForeignDocument, object>> foreignField,
            Expression<Func<TOutput, object>> @as,
            AggregateLookupOptions<TForeignDocument, TOutput> options = null)
        {
            Ensure.IsNotNull(foreignCollectionName, nameof(foreignCollectionName));
            Ensure.IsNotNull(localField, nameof(localField));
            Ensure.IsNotNull(foreignField, nameof(foreignField));
            Ensure.IsNotNull(@as, nameof(@as));

            return Lookup(
                foreignCollectionName,
                new ExpressionFieldDefinition<TInput>(localField),
                new ExpressionFieldDefinition<TForeignDocument>(foreignField),
                new ExpressionFieldDefinition<TOutput>(@as),
                options);
        }

        /// <summary>
        /// Creates a $match stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <param name="filter">The filter.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TInput> Match<TInput>(
            FilterDefinition<TInput> filter)
        {
            const string operatorName = "$match";
            var stage = new DelegatedPipelineStageDefinition<TInput, TInput>(
                operatorName,
                (s, sr) => new RenderedPipelineStageDefinition<TInput>(operatorName, new BsonDocument(operatorName, filter.Render(s, sr)), s));

            return stage;
        }

        /// <summary>
        /// Creates a $match stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <param name="filter">The filter.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TInput> Match<TInput>(
            Expression<Func<TInput, bool>> filter)
        {
            Ensure.IsNotNull(filter, nameof(filter));

            var filterDefinition = new ExpressionFilterDefinition<TInput>(filter);
            return Match(filterDefinition);
        }

        /// <summary>
        /// Create a $match stage that select documents of a sub type.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="outputSerializer">The output serializer.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> OfType<TInput, TOutput>(
            IBsonSerializer<TOutput> outputSerializer = null)
                where TOutput : TInput
        {
            var discriminatorConvention = BsonSerializer.LookupDiscriminatorConvention(typeof(TOutput));
            if (discriminatorConvention == null)
            {
                var message = string.Format("OfType requires that a discriminator convention exist for type: {0}.", BsonUtils.GetFriendlyTypeName(typeof(TOutput)));
                throw new NotSupportedException(message);
            }

            var discriminatorValue = discriminatorConvention.GetDiscriminator(typeof(TInput), typeof(TOutput));
            var ofTypeFilter = new BsonDocument(discriminatorConvention.ElementName, discriminatorValue);

            const string operatorName = "$match";
            var stage = new DelegatedPipelineStageDefinition<TInput, TOutput>(
                operatorName,
                (s, sr) =>
                {
                    return new RenderedPipelineStageDefinition<TOutput>(
                        operatorName,
                        new BsonDocument(operatorName, ofTypeFilter),
                        outputSerializer ?? (s as IBsonSerializer<TOutput>) ?? sr.GetSerializer<TOutput>());
                });

            return stage;
        }

        /// <summary>
        /// Creates a $out stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <param name="outputCollectionName">The output collection.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TInput> Out<TInput>(
            string outputCollectionName)
        {
            return new BsonDocumentPipelineStageDefinition<TInput, TInput>(new BsonDocument("$out", outputCollectionName));
        }

        /// <summary>
        /// Creates a $project stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="projection">The projection.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> Project<TInput, TOutput>(
            ProjectionDefinition<TInput, TOutput> projection)
        {
            const string operatorName = "$project";
            var stage = new DelegatedPipelineStageDefinition<TInput, TOutput>(
                operatorName,
                (s, sr) =>
                {
                    var renderedProjection = projection.Render(s, sr);
                    BsonDocument document;
                    if (renderedProjection.Document == null)
                    {
                        document = new BsonDocument();
                    }
                    else
                    {
                        document = new BsonDocument(operatorName, renderedProjection.Document);
                    }
                    return new RenderedPipelineStageDefinition<TOutput>(operatorName, document, renderedProjection.ProjectionSerializer);
                });

            return stage;
        }

        /// <summary>
        /// Creates a $project stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <param name="projection">The projection.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, BsonDocument> Project<TInput>(
            ProjectionDefinition<TInput, BsonDocument> projection)
        {
            Ensure.IsNotNull(projection, nameof(projection));

            return Project<TInput, BsonDocument>(projection);
        }

        /// <summary>
        /// Creates a $project stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="projection">The projection.</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> Project<TInput, TOutput>(
            Expression<Func<TInput, TOutput>> projection,
            ExpressionTranslationOptions translationOptions = null)
        {
            Ensure.IsNotNull(projection, nameof(projection));

            var projectionDefinition = new ProjectExpressionProjection<TInput, TOutput>(projection, translationOptions);
            return Project(projectionDefinition);
        }

        /// <summary>
        /// Creates a $replaceRoot stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="newRoot">The new root.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> ReplaceRoot<TInput, TOutput>(
            AggregateExpressionDefinition<TInput, TOutput> newRoot)
        {
            const string operatorName = "$replaceRoot";
            var stage = new DelegatedPipelineStageDefinition<TInput, TOutput>(
                operatorName,
                (s, sr) =>
                {
                    var document = new BsonDocument(operatorName, new BsonDocument("newRoot", newRoot.Render(s, sr)));
                    var outputSerializer = sr.GetSerializer<TOutput>();
                    return new RenderedPipelineStageDefinition<TOutput>(operatorName, document, outputSerializer);
                });

            return stage;
        }

        /// <summary>
        /// Creates a $replaceRoot stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="newRoot">The new root.</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> ReplaceRoot<TInput, TOutput>(
            Expression<Func<TInput, TOutput>> newRoot,
            ExpressionTranslationOptions translationOptions = null)
        {
            Ensure.IsNotNull(newRoot, nameof(newRoot));

            var newRootDefinition = new ExpressionAggregateExpressionDefinition<TInput, TOutput>(newRoot, translationOptions);
            return ReplaceRoot(newRootDefinition);
        }

        /// <summary>
        /// Creates a $skip stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <param name="skip">The skip.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TInput> Skip<TInput>(
            int skip)
        {
            return new BsonDocumentPipelineStageDefinition<TInput, TInput>(new BsonDocument("$skip", skip));
        }

        /// <summary>
        /// Creates a $sort stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <param name="sort">The sort.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TInput> Sort<TInput>(
            SortDefinition<TInput> sort)
        {
            return new SortPipelineStageDefinition<TInput>(sort);
        }

        /// <summary>
        /// Creates a $sort stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <param name="field">The field.</param>
        /// <returns>
        /// The stage.
        /// </returns>
        public static PipelineStageDefinition<TInput, TInput> SortAscending<TInput>(
            Expression<Func<TInput, object>> field)
        {
            return Sort(Builders<TInput>.Sort.Ascending(field));
        }

        /// <summary>
        /// Creates a $sortByCount stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <param name="value">The value expression.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, AggregateSortByCountResult<TValue>> SortByCount<TInput, TValue>(
            AggregateExpressionDefinition<TInput, TValue> value)
        {
            const string operatorName = "$sortByCount";
            var stage = new DelegatedPipelineStageDefinition<TInput, AggregateSortByCountResult<TValue>>(
                operatorName,
                (s, sr) =>
                {
                    var outputSerializer = sr.GetSerializer<AggregateSortByCountResult<TValue>>();
                    return new RenderedPipelineStageDefinition<AggregateSortByCountResult<TValue>>(operatorName, new BsonDocument(operatorName, value.Render(s, sr)), outputSerializer);
                });

            return stage;
        }

        /// <summary>
        /// Creates a $sortByCount stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <param name="value">The value.</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, AggregateSortByCountResult<TValue>> SortByCount<TInput, TValue>(
            Expression<Func<TInput, TValue>> value,
            ExpressionTranslationOptions translationOptions = null)
        {
            Ensure.IsNotNull(value, nameof(value));

            var idDefinition = new ExpressionAggregateExpressionDefinition<TInput, TValue>(value, translationOptions);
            return SortByCount(idDefinition);
        }

        /// <summary>
        /// Creates a $sort stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <param name="field">The field.</param>
        /// <returns>
        /// The stage.
        /// </returns>
        public static PipelineStageDefinition<TInput, TInput> SortDescending<TInput>(
            Expression<Func<TInput, object>> field)
        {
            return Sort(Builders<TInput>.Sort.Descending(field));
        }

        /// <summary>
        /// Creates an $unwind stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="field">The field.</param>
        /// <param name="options">The options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> Unwind<TInput, TOutput>(
            FieldDefinition<TInput> field,
            AggregateUnwindOptions<TOutput> options = null)
        {
            options = options ?? new AggregateUnwindOptions<TOutput>();

            const string operatorName = "$unwind";
            var stage = new DelegatedPipelineStageDefinition<TInput, TOutput>(
                operatorName,
                (s, sr) =>
                {
                    var newResultSerializer = options.ResultSerializer ?? (s as IBsonSerializer<TOutput>) ?? sr.GetSerializer<TOutput>();

                    var fieldName = "$" + field.Render(s, sr).FieldName;
                    string includeArrayIndexFieldName = null;
                    if (options.IncludeArrayIndex != null)
                    {
                        includeArrayIndexFieldName = options.IncludeArrayIndex.Render(newResultSerializer, sr).FieldName;
                    }

                    BsonValue value = fieldName;
                    if (options.PreserveNullAndEmptyArrays.HasValue || includeArrayIndexFieldName != null)
                    {
                        value = new BsonDocument
                        {
                            { "path", fieldName },
                            { "preserveNullAndEmptyArrays", options.PreserveNullAndEmptyArrays, options.PreserveNullAndEmptyArrays.HasValue },
                            { "includeArrayIndex", includeArrayIndexFieldName, includeArrayIndexFieldName != null }
                        };
                    }
                    return new RenderedPipelineStageDefinition<TOutput>(
                        operatorName,
                        new BsonDocument(operatorName, value),
                        newResultSerializer);
                });

            return stage;
        }

        /// <summary>
        /// Creates an $unwind stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <param name="field">The field to unwind.</param>
        /// <param name="options">The options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, BsonDocument> Unwind<TInput>(
            FieldDefinition<TInput> field,
            AggregateUnwindOptions<BsonDocument> options = null)
        {
            Ensure.IsNotNull(field, nameof(field));

            return Unwind<TInput, BsonDocument>(field, options);
        }

        /// <summary>
        /// Creates an $unwind stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <param name="field">The field to unwind.</param>
        /// <param name="options">The options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, BsonDocument> Unwind<TInput>(
            Expression<Func<TInput, object>> field,
            AggregateUnwindOptions<BsonDocument> options = null)
        {
            Ensure.IsNotNull(field, nameof(field));

            var fieldDefinition = new ExpressionFieldDefinition<TInput>(field);
            return Unwind(fieldDefinition, options);
        }

        /// <summary>
        /// Creates an $unwind stage.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="field">The field to unwind.</param>
        /// <param name="options">The options.</param>
        /// <returns>The stage.</returns>
        public static PipelineStageDefinition<TInput, TOutput> Unwind<TInput, TOutput>(
            Expression<Func<TInput, object>> field,
            AggregateUnwindOptions<TOutput> options = null)
        {
            Ensure.IsNotNull(field, nameof(field));

            var fieldDefinition = new ExpressionFieldDefinition<TInput>(field);
            return Unwind(fieldDefinition, options);
        }
    }

    internal sealed class ExpressionBucketOutputProjection<TResult, TValue, TNewResult> : ProjectionDefinition<TResult, TNewResult>
    {
        private readonly Expression<Func<IGrouping<TValue, TResult>, TNewResult>> _outputExpression;
        private readonly ExpressionTranslationOptions _translationOptions;
        private readonly Expression<Func<TResult, TValue>> _valueExpression;

        public ExpressionBucketOutputProjection(
            Expression<Func<TResult, TValue>> valueExpression,
            Expression<Func<IGrouping<TValue, TResult>, TNewResult>> outputExpression,
            ExpressionTranslationOptions translationOptions)
        {
            _valueExpression = Ensure.IsNotNull(valueExpression, nameof(valueExpression));
            _outputExpression = Ensure.IsNotNull(outputExpression, nameof(outputExpression));
            _translationOptions = translationOptions; // can be null

        }

        public Expression<Func<IGrouping<TValue, TResult>, TNewResult>> OutputExpression
        {
            get { return _outputExpression; }
        }

        public override RenderedProjectionDefinition<TNewResult> Render(IBsonSerializer<TResult> documentSerializer, IBsonSerializerRegistry serializerRegistry)
        {
            var renderedOutput = AggregateGroupTranslator.Translate<TValue, TResult, TNewResult>(_valueExpression, _outputExpression, documentSerializer, serializerRegistry, _translationOptions);
            var document = renderedOutput.Document;
            document.Remove("_id");
            return new RenderedProjectionDefinition<TNewResult>(document, renderedOutput.ProjectionSerializer);
        }
    }

    internal sealed class GroupExpressionProjection<TResult, TKey, TNewResult> : ProjectionDefinition<TResult, TNewResult>
    {
        private readonly Expression<Func<TResult, TKey>> _idExpression;
        private readonly Expression<Func<IGrouping<TKey, TResult>, TNewResult>> _groupExpression;
        private readonly ExpressionTranslationOptions _translationOptions;

        public GroupExpressionProjection(Expression<Func<TResult, TKey>> idExpression, Expression<Func<IGrouping<TKey, TResult>, TNewResult>> groupExpression, ExpressionTranslationOptions translationOptions)
        {
            _idExpression = Ensure.IsNotNull(idExpression, nameof(idExpression));
            _groupExpression = Ensure.IsNotNull(groupExpression, nameof(groupExpression));
            _translationOptions = translationOptions; // can be null
        }

        public Expression<Func<TResult, TKey>> IdExpression
        {
            get { return _idExpression; }
        }

        public Expression<Func<IGrouping<TKey, TResult>, TNewResult>> GroupExpression
        {
            get { return _groupExpression; }
        }

        public override RenderedProjectionDefinition<TNewResult> Render(IBsonSerializer<TResult> documentSerializer, IBsonSerializerRegistry serializerRegistry)
        {
            return AggregateGroupTranslator.Translate<TKey, TResult, TNewResult>(_idExpression, _groupExpression, documentSerializer, serializerRegistry, _translationOptions);
        }
    }

    internal sealed class ProjectExpressionProjection<TResult, TNewResult> : ProjectionDefinition<TResult, TNewResult>
    {
        private readonly Expression<Func<TResult, TNewResult>> _expression;
        private readonly ExpressionTranslationOptions _translationOptions;

        public ProjectExpressionProjection(Expression<Func<TResult, TNewResult>> expression, ExpressionTranslationOptions translationOptions)
        {
            _expression = Ensure.IsNotNull(expression, nameof(expression));
            _translationOptions = translationOptions; // can be null
        }

        public Expression<Func<TResult, TNewResult>> Expression
        {
            get { return _expression; }
        }

        public override RenderedProjectionDefinition<TNewResult> Render(IBsonSerializer<TResult> documentSerializer, IBsonSerializerRegistry serializerRegistry)
        {
            return AggregateProjectTranslator.Translate<TResult, TNewResult>(_expression, documentSerializer, serializerRegistry, _translationOptions);
        }
    }

    internal class SortPipelineStageDefinition<TInput> : PipelineStageDefinition<TInput, TInput>
    {
        public SortPipelineStageDefinition(SortDefinition<TInput> sort)
        {
            Sort = sort;
        }

        public SortDefinition<TInput> Sort { get; private set; }

        public override string OperatorName => "$sort";

        public override RenderedPipelineStageDefinition<TInput> Render(IBsonSerializer<TInput> inputSerializer, IBsonSerializerRegistry serializerRegistry)
        {
            var renderedSort = Sort.Render(inputSerializer, serializerRegistry);
            var document = new BsonDocument(OperatorName, renderedSort);
            return new RenderedPipelineStageDefinition<TInput>(OperatorName, document, inputSerializer);
        }
    }
}
