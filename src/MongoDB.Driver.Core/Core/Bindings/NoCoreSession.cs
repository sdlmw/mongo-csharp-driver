﻿/* Copyright 2017 MongoDB Inc.
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

using MongoDB.Bson;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Operations;

namespace MongoDB.Driver.Core.Bindings
{
    /// <summary>
    /// An object that represents no core session.
    /// </summary>
    /// <seealso cref="MongoDB.Driver.Core.Bindings.ICoreSession" />
    public sealed class NoCoreSession : ICoreSession
    {
        #region static
        // private static fields
        private static readonly ICoreSession __instance = new NoCoreSession();

        // public static properties
        /// <summary>
        /// Gets the pre-created instance.
        /// </summary>
        /// <value>
        /// The instance.
        /// </value>
        public static ICoreSession Instance => __instance;
        #endregion

        // private fields
        private readonly IClusterClock _clusterClock = new NoClusterClock();
        private readonly IOperationClock _operationClock = new NoOperationClock();

        // public properties
        /// <inheritdoc />
        public IClusterClock ClusterClock => _clusterClock;

        /// <inheritdoc />
        public BsonDocument Id => null;

        /// <inheritdoc />
        public bool IsImplicitSession => true;

        /// <inheritdoc />
        public IOperationClock OperationClock => _operationClock;

        /// <inheritdoc />
        public void WasUsed()
        {
        }
    }
}
