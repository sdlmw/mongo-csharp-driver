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

using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver
{
    /// <summary>
    /// Represents static methods related to creating facets.
    /// </summary>
    public static class AggregateFacet
    {
        /// <summary>
        /// Creates a new instance of the <see cref="AggregateFacet{TDocument, TResult}"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="pipeline">The pipeline.</param>
        /// <returns>A new instance of the <see cref="AggregateFacet{TDocument, TResult}"/> class</returns>
        public static AggregateFacet<TDocument, TResult> Create<TDocument, TResult>(string name, PipelineDefinition<TDocument, TResult> pipeline)
        {
            return new AggregateFacet<TDocument, TResult>(name, pipeline);
        }
    }

    /// <summary>
    /// Represents a facet to be passed to the Facet method.
    /// </summary>
    /// <typeparam name="TDocument">The type of the input document.</typeparam>
    /// <typeparam name="TResult">The type of the facet result.</typeparam>
    public class AggregateFacet<TDocument, TResult>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AggregateFacet{TDocument, TResult}"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="pipeline">The pipeline.</param>
        public AggregateFacet(string name, PipelineDefinition<TDocument, TResult> pipeline)
        {
            Name = Ensure.IsNotNull(name, nameof(name));
            Pipeline = Ensure.IsNotNull(pipeline, nameof(pipeline));
        }

        /// <summary>
        /// Gets the name.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Gets the pipeline.
        /// </summary>
        public PipelineDefinition<TDocument, TResult> Pipeline { get; private set; }
    }
}
