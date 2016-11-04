﻿/* Copyright 2016 MongoDB Inc.
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

using MongoDB.Bson.Serialization;

namespace MongoDB.Driver
{
    /// <summary>
    /// Represents options for the GraphLookup method.
    /// </summary>
    public sealed class AggregateGraphLookupOptions<TNewResult, TOutput, TFrom, TConnect>
    {
        /// <summary>
        /// Gets or sets the depth field.
        /// </summary>
        public FieldDefinition<TOutput, int> DepthField { get; set; }

        /// <summary>
        /// Gets or sets the TFrom serializer.
        /// </summary>
        public IBsonSerializer<TFrom> FromSerializer { get; set; }

        /// <summary>
        /// Gets or sets the maximum depth.
        /// </summary>
        public int? MaxDepth { get; set; }

        /// <summary>
        /// Gets or sets the TOutput serialzier.
        /// </summary>
        public IBsonSerializer<TOutput> OutputSerialzier { get; set; }

        /// <summary>
        /// Gets the filter to restrict the search with.
        /// </summary>
        public FilterDefinition<TFrom> RestrictSearchWithMatch { get; set; }

        /// <summary>
        /// Gets or sets the TNewResult serializer.
        /// </summary>
        public IBsonSerializer<TNewResult> ResultSerializer { get; set; }
    }
}
