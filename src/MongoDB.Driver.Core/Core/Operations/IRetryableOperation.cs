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

namespace MongoDB.Driver.Core.Operations
{
    /// <summary>
    /// Represents a retryable operation.
    /// </summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    public interface IRetryableWriteOperation<TResult>
    {
        /// <summary>
        /// Executes the operation.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns></returns>
        TResult Execute(RetryableWriteContext context, CancellationToken cancellationToken);

        /// <summary>
        /// Executes the initial attempt.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="transactionNumber">The transaction number.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns></returns>
        TResult ExecuteInitialAttempt(RetryableWriteContext context, long? transactionNumber, CancellationToken cancellationToken);

        /// <summary>
        /// Executes the retry.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="transactionNumber">The transaction number.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns></returns>
        TResult ExecuteRetry(RetryableWriteContext context, long transactionNumber, CancellationToken cancellationToken);
    }
}
