/* Copyright 2017 MongoDB Inc.
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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Connections;

namespace MongoDB.Driver.Core.Operations
{
    internal static class OperationHelper
    {
        public static bool IsRetryableWrite(bool retryOnFailure, WriteConcern writeConcern, ConnectionDescription connectionDescription)
        {
            if (!retryOnFailure)
            {
                return false;
            }

            if (!writeConcern.IsAcknowledged)
            {
                return false;
            }

            return CanRetryWrite(connectionDescription);

        }

        private static bool CanRetryWrite(ConnectionDescription connectionDescription)
        {
            if (connectionDescription.IsMasterResult.LogicalSessionTimeout == null)
            {
                return false;
            }

            if (connectionDescription.IsMasterResult.ServerType == Servers.ServerType.Standalone)
            {
                return false;
            }

            return true;
        }

        public static bool IsRetryableException(Exception ex)
        {
            return ex is MongoNotPrimaryException ||
                ex is MongoConnectionException ||
                ex is MongoNodeIsRecoveringException;
        }
    }
}
