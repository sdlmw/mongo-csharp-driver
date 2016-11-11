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
using System.Linq.Expressions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver
{
    /// <summary>
    /// Extension methods for adding stages to a pipeline.
    /// </summary>
    public static class PipelineDefinitionBuilder
    {
        /// <summary>
        /// Appends a stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="stage">The stage.</param>
        /// <param name="newOutputSerializer">The new output serializer.</param>
        /// <returns>A new pipeline with an additional stage.</returns>
        public static PipelineDefinition<TInput, TNewOutput> AppendStage<TInput, TOutput, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            PipelineStageDefinition<TOutput, TNewOutput> stage,
            IBsonSerializer<TNewOutput> newOutputSerializer = null)
        {
            return new AppendedStagePipelineDefinition<TInput, TOutput, TNewOutput>(pipeline, stage, newOutputSerializer);
        }

        /// <summary>
        /// Changes the output type of the pipeline by using a new output serializer.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="newOutputSerializer">The new output serializer.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> As<TInput, TOutput, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            IBsonSerializer<TNewOutput> newOutputSerializer)
        {
            return new NewOutputSerializerPipelineDefinition<TInput, TOutput, TNewOutput>(pipeline, newOutputSerializer);
        }

        /// <summary>
        /// Appends a $bucket stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="boundaries">The boundaries.</param>
        /// <param name="options">The options.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, AggregateBucketResult<TValue>> Bucket<TInput, TOutput, TValue>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            AggregateExpressionDefinition<TOutput, TValue> groupBy,
            IEnumerable<TValue> boundaries,
            AggregateBucketOptions<TValue> options = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Bucket(groupBy, boundaries, options));
        }

        /// <summary>
        /// Appends a $bucket stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="boundaries">The boundaries.</param>
        /// <param name="output">The output projection.</param>
        /// <param name="options">The options.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> Bucket<TInput, TOutput, TValue, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            AggregateExpressionDefinition<TOutput, TValue> groupBy,
            IEnumerable<TValue> boundaries,
            ProjectionDefinition<TOutput, TNewOutput> output,
            AggregateBucketOptions<TValue> options = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Bucket(groupBy, boundaries, output, options));
        }

        /// <summary>
        /// Appends a $bucket stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="boundaries">The boundaries.</param>
        /// <param name="options">The options.</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, AggregateBucketResult<TValue>> Bucket<TInput, TOutput, TValue, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            Expression<Func<TOutput, TValue>> groupBy,
            IEnumerable<TValue> boundaries,
            AggregateBucketOptions<TValue> options = null,
            ExpressionTranslationOptions translationOptions = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Bucket(groupBy, boundaries, options, translationOptions));
        }

        /// <summary>
        /// Appends a $bucket stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="boundaries">The boundaries.</param>
        /// <param name="output">The output projection.</param>
        /// <param name="options">The options.</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> Bucket<TInput, TOutput, TValue, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            Expression<Func<TOutput, TValue>> groupBy,
            IEnumerable<TValue> boundaries,
            Expression<Func<IGrouping<TValue, TOutput>, TNewOutput>> output,
            AggregateBucketOptions<TValue> options = null,
            ExpressionTranslationOptions translationOptions = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Bucket(groupBy, boundaries, output, options, translationOptions));
        }

        /// <summary>
        /// Appends a $bucketAuto stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="buckets">The number of buckets.</param>
        /// <param name="options">The options.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, AggregateBucketAutoResult<TValue>> BucketAuto<TInput, TOutput, TValue>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            AggregateExpressionDefinition<TOutput, TValue> groupBy,
            int buckets,
            AggregateBucketAutoOptions options = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.BucketAuto(groupBy, buckets, options));
        }

        /// <summary>
        /// Appends a $bucketAuto stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="buckets">The number of buckets.</param>
        /// <param name="output">The output projection.</param>
        /// <param name="options">The options.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> BucketAuto<TInput, TOutput, TValue, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            AggregateExpressionDefinition<TOutput, TValue> groupBy,
            int buckets,
            ProjectionDefinition<TOutput, TNewOutput> output,
            AggregateBucketAutoOptions options = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.BucketAuto(groupBy, buckets, output, options));
        }

        /// <summary>
        /// Appends a $bucketAuto stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="buckets">The number of buckets.</param>
        /// <param name="options">The options (optional).</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, AggregateBucketAutoResult<TValue>> BucketAuto<TInput, TOutput, TValue>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            Expression<Func<TOutput, TValue>> groupBy,
            int buckets,
            AggregateBucketAutoOptions options = null,
            ExpressionTranslationOptions translationOptions = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.BucketAuto(groupBy, buckets, options, translationOptions));
        }

        /// <summary>
        /// Appends a $bucketAuto stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="groupBy">The group by expression.</param>
        /// <param name="buckets">The number of buckets.</param>
        /// <param name="output">The output projection.</param>
        /// <param name="options">The options (optional).</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> BucketAuto<TInput, TOutput, TValue, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            Expression<Func<TOutput, TValue>> groupBy,
            int buckets,
            Expression<Func<IGrouping<TValue, TOutput>, TNewOutput>> output,
            AggregateBucketAutoOptions options = null,
            ExpressionTranslationOptions translationOptions = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.BucketAuto(groupBy, buckets, output, options, translationOptions));
        }

        /// <summary>
        /// Appends a $count stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, AggregateCountResult> Count<TInput, TOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Count<TOutput>());
        }

        /// <summary>
        /// Appends a $facet stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="facets">The facets.</param>
        /// <param name="options">The options.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> Facet<TInput, TOutput, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            IEnumerable<AggregateFacet<TOutput>> facets,
            AggregateFacetOptions<TNewOutput> options = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Facet(facets, options));
        }

        /// <summary>
        /// Appends a $facet stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="facets">The facets.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, AggregateFacetResults> Facet<TInput, TOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            IEnumerable<AggregateFacet<TOutput>> facets)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Facet(facets));
        }

        /// <summary>
        /// Appends a $facet stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="facets">The facets.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, AggregateFacetResults> Facet<TInput, TOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            params AggregateFacet<TOutput>[] facets)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Facet(facets));
        }

        /// <summary>
        /// Appends a $facet stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="facets">The facets.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> Facet<TInput, TOutput, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            params AggregateFacet<TOutput>[] facets)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Facet<TOutput, TNewOutput>(facets));
        }

        /// <summary>
        /// Used to start creating a pipeline for {TInput} documents.
        /// </summary>
        /// <typeparam name="TInput">The type of the output.</typeparam>
        /// <param name="inputSerializer">The inputSerializer serializer.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, TInput> For<TInput>(IBsonSerializer<TInput> inputSerializer = null)
        {
            return new EmptyPipelineDefinition<TInput>(inputSerializer);
        }

        /// <summary>
        /// Appends a $group stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="group">The group projection.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> Group<TInput, TOutput, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            ProjectionDefinition<TOutput, TNewOutput> group)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Group(group));
        }

        /// <summary>
        /// Appends a group stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="group">The group projection.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, BsonDocument> Group<TInput, TOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            ProjectionDefinition<TOutput, BsonDocument> group)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Group(group));
        }

        /// <summary>
        /// Appends a group stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="id">The id.</param>
        /// <param name="group">The group projection.</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> Group<TInput, TOutput, TKey, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            Expression<Func<TOutput, TKey>> id,
            Expression<Func<IGrouping<TKey, TOutput>, TNewOutput>> group,
            ExpressionTranslationOptions translationOptions = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Group(id, group, translationOptions));
        }

        /// <summary>
        /// Appends a $limit stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="limit">The limit.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, TOutput> Limit<TInput, TOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            int limit)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Limit<TOutput>(limit));
        }

        /// <summary>
        /// Appends a $lookup stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TForeignDocument">The type of the foreign collection documents.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="foreignCollectionName">The foreign collection.</param>
        /// <param name="localField">The local field.</param>
        /// <param name="foreignField">The foreign field.</param>
        /// <param name="as">The "as" field.</param>
        /// <param name="options">The options.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> Lookup<TInput, TOutput, TForeignDocument, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            string foreignCollectionName,
            FieldDefinition<TOutput> localField,
            FieldDefinition<TForeignDocument> foreignField,
            FieldDefinition<TNewOutput> @as,
            AggregateLookupOptions<TForeignDocument, TNewOutput> options = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Lookup(foreignCollectionName, localField, foreignField, @as, options));
        }

        /// <summary>
        /// Appends a lookup stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TForeignDocument">The type of the foreign collection documents.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="foreignCollectionName">The foreign collection.</param>
        /// <param name="localField">The local field.</param>
        /// <param name="foreignField">The foreign field.</param>
        /// <param name="as">The "as" field.</param>
        /// <param name="options">The options.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> Lookup<TInput, TOutput, TForeignDocument, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            string foreignCollectionName,
            Expression<Func<TOutput, object>> localField,
            Expression<Func<TForeignDocument, object>> foreignField,
            Expression<Func<TNewOutput, object>> @as,
            AggregateLookupOptions<TForeignDocument, TNewOutput> options = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Lookup(foreignCollectionName, localField, foreignField, @as, options));
        }

        /// <summary>
        /// Appends a $match stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="filter">The filter.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, TOutput> Match<TInput, TOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            FilterDefinition<TOutput> filter)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Match(filter));
        }

        /// <summary>
        /// Appends a match stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="filter">The filter.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, TOutput> Match<TInput, TOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            Expression<Func<TOutput, bool>> filter)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Match(filter));
        }

        /// <summary>
        /// Appends a $match stage to the pipeline to select documents of a certain type.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="newOutputSerializer">The new output serializer.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        /// <exception cref="System.NotSupportedException"></exception>
        public static PipelineDefinition<TInput, TNewOutput> OfType<TInput, TOutput, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            IBsonSerializer<TNewOutput> newOutputSerializer = null)
                where TNewOutput : TOutput
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.OfType<TOutput, TNewOutput>(newOutputSerializer));
        }

        /// <summary>
        /// Appends a $out stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="outputCollectionName">The output collection.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        /// <exception cref="System.NotSupportedException"></exception>
        public static PipelineDefinition<TInput, TOutput> Out<TInput, TOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            string outputCollectionName)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Out<TOutput>(outputCollectionName));
        }

        /// <summary>
        /// Appends a $project stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="projection">The projection.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        /// <exception cref="System.NotSupportedException"></exception>
        public static PipelineDefinition<TInput, TNewOutput> Project<TInput, TOutput, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            ProjectionDefinition<TOutput, TNewOutput> projection)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Project(projection));
        }

        /// <summary>
        /// Appends a project stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="projection">The projection.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, BsonDocument> Project<TInput, TOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            ProjectionDefinition<TOutput, BsonDocument> projection)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Project(projection));
        }

        /// <summary>
        /// Appends a project stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="projection">The projection.</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> Project<TInput, TOutput, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            Expression<Func<TOutput, TNewOutput>> projection,
            ExpressionTranslationOptions translationOptions = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Project(projection, translationOptions));
        }

        /// <summary>
        /// Appends a $replaceRoot stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="newRoot">The new root.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> ReplaceRoot<TInput, TOutput, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            AggregateExpressionDefinition<TOutput, TNewOutput> newRoot)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.ReplaceRoot(newRoot));
        }

        /// <summary>
        /// Appends a $replaceRoot stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="newRoot">The new root.</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> ReplaceRoot<TInput, TOutput, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            Expression<Func<TOutput, TNewOutput>> newRoot,
            ExpressionTranslationOptions translationOptions = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.ReplaceRoot(newRoot, translationOptions));
        }

        /// <summary>
        /// Appends a $skip stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="skip">The number of documents to skip.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, TOutput> Skip<TInput, TOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            int skip)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Skip<TOutput>(skip));
        }

        /// <summary>
        /// Appends a $sort stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="sort">The sort definition.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, TOutput> Sort<TInput, TOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            SortDefinition<TOutput> sort)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Sort(sort));
        }

        /// <summary>
        /// Appends a $sortByCount stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="value">The value expression.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, AggregateSortByCountResult<TValue>> SortByCount<TInput, TOutput, TValue>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            AggregateExpressionDefinition<TOutput, TValue> value)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.SortByCount(value));
        }

        /// <summary>
        /// Appends a sortByCount stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="value">The value expression.</param>
        /// <param name="translationOptions">The translation options.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, AggregateSortByCountResult<TValue>> SortByCount<TInput, TOutput, TValue>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            Expression<Func<TOutput, TValue>> value,
            ExpressionTranslationOptions translationOptions)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.SortByCount(value, translationOptions));
        }

        /// <summary>
        /// Appends an $unwind stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="field">The field.</param>
        /// <param name="options">The options.</param>
        /// <returns>
        /// A new pipeline with an additional stage.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> Unwind<TInput, TOutput, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            FieldDefinition<TOutput> field,
            AggregateUnwindOptions<TNewOutput> options = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Unwind(field, options));
        }

        /// <summary>
        /// Appends an unwind stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="field">The field to unwind.</param>
        /// <param name="options">The options.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, BsonDocument> Unwind<TInput, TOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            FieldDefinition<TOutput> field,
            AggregateUnwindOptions<BsonDocument> options = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Unwind(field, options));
        }

        /// <summary>
        /// Appends an unwind stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="field">The field to unwind.</param>
        /// <param name="options">The options.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, BsonDocument> Unwind<TInput, TOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            Expression<Func<TOutput, object>> field,
            AggregateUnwindOptions<BsonDocument> options = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Unwind(field, options));
        }

        /// <summary>
        /// Appends an unwind stage to the pipeline.
        /// </summary>
        /// <typeparam name="TInput">The type of the input documents.</typeparam>
        /// <typeparam name="TOutput">The type of the output documents.</typeparam>
        /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="field">The field to unwind.</param>
        /// <param name="options">The options.</param>
        /// <returns>
        /// The fluent aggregate interface.
        /// </returns>
        public static PipelineDefinition<TInput, TNewOutput> Unwind<TInput, TOutput, TNewOutput>(
            this PipelineDefinition<TInput, TOutput> pipeline,
            Expression<Func<TOutput, object>> field,
            AggregateUnwindOptions<TNewOutput> options = null)
        {
            Ensure.IsNotNull(pipeline, nameof(pipeline));
            return pipeline.AppendStage(PipelineStageDefinitionBuilder.Unwind(field, options));
        }
    }

    /// <summary>
    /// Represents a pipeline consisting of an existing pipeline with one additional stage appended.
    /// </summary>
    /// <typeparam name="TInput">The type of the input documents.</typeparam>
    /// <typeparam name="TOutput">The type of the output documents.</typeparam>
    /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
    public sealed class AppendedStagePipelineDefinition<TInput, TOutput, TNewOutput> : PipelineDefinition<TInput, TNewOutput>
    {
        private readonly IBsonSerializer<TNewOutput> _newOutputSerializer;
        private readonly PipelineDefinition<TInput, TOutput> _pipeline;
        private readonly PipelineStageDefinition<TOutput, TNewOutput> _stage;

        /// <summary>
        /// Initializes a new instance of the <see cref="AppendedStagePipelineDefinition{TInput, TOutput, TNewOutput}" /> class.
        /// </summary>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="stage">The stage.</param>
        /// <param name="newOutputSerializer">The new output serializer.</param>
        public AppendedStagePipelineDefinition(
            PipelineDefinition<TInput, TOutput> pipeline,
            PipelineStageDefinition<TOutput, TNewOutput> stage,
            IBsonSerializer<TNewOutput> newOutputSerializer = null)
        {
            _pipeline = Ensure.IsNotNull(pipeline, nameof(pipeline));
            _stage = Ensure.IsNotNull(stage, nameof(stage));
            _newOutputSerializer = newOutputSerializer; // can be null
        }

        /// <inheritdoc/>
        public override IBsonSerializer<TNewOutput> OutputSerializer => _newOutputSerializer;

        /// <inheritdoc/>
        public override IEnumerable<IPipelineStageDefinition> Stages => _pipeline.Stages.Concat(new[] { _stage });

        /// <inheritdoc/>
        public override RenderedPipelineDefinition<TNewOutput> Render(IBsonSerializer<TInput> inputSerializer, IBsonSerializerRegistry serializerRegistry)
        {
            var renderedPipeline = _pipeline.Render(inputSerializer, serializerRegistry);
            var renderedStage = _stage.Render(renderedPipeline.OutputSerializer, serializerRegistry);
            var documents = renderedPipeline.Documents.Concat(new[] { renderedStage.Document });
            var outputSerializer = _newOutputSerializer ?? renderedStage.OutputSerializer;
            return new RenderedPipelineDefinition<TNewOutput>(documents, outputSerializer);
        }
    }

    /// <summary>
    /// Represents an empty pipeline.
    /// </summary>
    /// <typeparam name="TInput">The type of the input documents.</typeparam>
    public sealed class EmptyPipelineDefinition<TInput> : PipelineDefinition<TInput, TInput>
    {
        private readonly IBsonSerializer<TInput> _inputSerializer;

        /// <summary>
        /// Initializes a new instance of the <see cref="EmptyPipelineDefinition{TOutput}"/> class.
        /// </summary>
        /// <param name="inputSerializer">The output serializer.</param>
        public EmptyPipelineDefinition(IBsonSerializer<TInput> inputSerializer = null)
        {
            _inputSerializer = inputSerializer; // can be null
        }

        /// <inheritdoc/>
        public override IBsonSerializer<TInput> OutputSerializer => _inputSerializer;

        /// <inheritdoc/>
        public override IEnumerable<IPipelineStageDefinition> Stages => Enumerable.Empty<IPipelineStageDefinition>();

        /// <inheritdoc/>
        public override RenderedPipelineDefinition<TInput> Render(IBsonSerializer<TInput> inputSerializer, IBsonSerializerRegistry serializerRegistry)
        {
            var documents = Enumerable.Empty<BsonDocument>();
            return new RenderedPipelineDefinition<TInput>(documents, _inputSerializer ?? inputSerializer);
        }
    }

    /// <summary>
    /// Represents a pipeline with the output serializer replaced.
    /// </summary>
    /// <typeparam name="TInput">The type of the input documents.</typeparam>
    /// <typeparam name="TOutput">The type of the output documents.</typeparam>
    /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
    /// <seealso cref="MongoDB.Driver.PipelineDefinition{TInput, TOutput}" />
    public sealed class NewOutputSerializerPipelineDefinition<TInput, TOutput, TNewOutput> : PipelineDefinition<TInput, TNewOutput>
    {
        private readonly IBsonSerializer<TNewOutput> _newOutputSerializer;
        private readonly PipelineDefinition<TInput, TOutput> _pipeline;

        /// <summary>
        /// Initializes a new instance of the <see cref="NewOutputSerializerPipelineDefinition{TInput, TInput, TNewOutput}"/> class.
        /// </summary>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="newOutputSerializer">The new output serializer.</param>
        public NewOutputSerializerPipelineDefinition(
            PipelineDefinition<TInput, TOutput> pipeline,
            IBsonSerializer<TNewOutput> newOutputSerializer)
        {
            _pipeline = Ensure.IsNotNull(pipeline, nameof(pipeline));
            _newOutputSerializer = newOutputSerializer; // can be null
        }

        /// <inheritdoc/>
        public override IBsonSerializer<TNewOutput> OutputSerializer => _newOutputSerializer;

        /// <inheritdoc/>
        public override IEnumerable<IPipelineStageDefinition> Stages => _pipeline.Stages;

        /// <inheritdoc/>
        public override RenderedPipelineDefinition<TNewOutput> Render(IBsonSerializer<TInput> inputSerializer, IBsonSerializerRegistry serializerRegistry)
        {
            var renderedPipeline = _pipeline.Render(inputSerializer, serializerRegistry);
            var outputSerializer = _newOutputSerializer ?? serializerRegistry.GetSerializer<TNewOutput>();
            return new RenderedPipelineDefinition<TNewOutput>(renderedPipeline.Documents, outputSerializer);
        }
    }

    /// <summary>
    /// Represents a pipeline consisting of an existing pipeline with one additional stage prepended.
    /// </summary>
    /// <typeparam name="TInput">The type of the input documents.</typeparam>
    /// <typeparam name="TOutput">The type of the output documents.</typeparam>
    /// <typeparam name="TNewOutput">The type of the new output documents.</typeparam>
    public sealed class PrependedStagePipelineDefinition<TInput, TOutput, TNewOutput> : PipelineDefinition<TInput, TNewOutput>
    {
        private readonly IBsonSerializer<TNewOutput> _newOutputSerializer;
        private readonly PipelineDefinition<TOutput, TNewOutput> _pipeline;
        private readonly PipelineStageDefinition<TInput, TOutput> _stage;

        /// <summary>
        /// Initializes a new instance of the <see cref="PrependedStagePipelineDefinition{TInput, TOutput, TNewOutput}" /> class.
        /// </summary>
        /// <param name="stage">The stage.</param>
        /// <param name="pipeline">The pipeline.</param>
        /// <param name="newOutputSerializer">The new output serializer.</param>
        public PrependedStagePipelineDefinition(
            PipelineStageDefinition<TInput, TOutput> stage,
            PipelineDefinition<TOutput, TNewOutput> pipeline,
            IBsonSerializer<TNewOutput> newOutputSerializer = null)
        {
            _stage = Ensure.IsNotNull(stage, nameof(stage));
            _pipeline = Ensure.IsNotNull(pipeline, nameof(pipeline));
            _newOutputSerializer = newOutputSerializer; // can be null
        }

        /// <inheritdoc/>
        public override IBsonSerializer<TNewOutput> OutputSerializer => _newOutputSerializer;

        /// <inheritdoc/>
        public override IEnumerable<IPipelineStageDefinition> Stages => new[] { _stage }.Concat(_pipeline.Stages);

        /// <inheritdoc/>
        public override RenderedPipelineDefinition<TNewOutput> Render(IBsonSerializer<TInput> inputSerializer, IBsonSerializerRegistry serializerRegistry)
        {
            var renderedStage = _stage.Render(inputSerializer, serializerRegistry);
            var renderedPipeline = _pipeline.Render(renderedStage.OutputSerializer, serializerRegistry);
            var documents = new[] { renderedStage.Document }.Concat(renderedPipeline.Documents);
            var newOutputSerializer = _newOutputSerializer ?? renderedPipeline.OutputSerializer;
            return new RenderedPipelineDefinition<TNewOutput>(documents, newOutputSerializer);
        }
    }
}
