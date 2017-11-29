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

using System.Threading;
using System.Threading.Tasks;

namespace MongoDB.Driver.Core.Operations
{
    /// <summary>
    /// Represents a retryable operation.
    /// </summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    public interface IRetryableWriteOperation<TResult>
    {
        /// <summary>
        /// Executes the first attempt.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="attempt">The attempt.</param>
        /// <param name="transactionNumber">The transaction number.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result.</returns>
        TResult ExecuteAttempt(RetryableWriteOperationContext context, int attempt, long? transactionNumber, CancellationToken cancellationToken);

        /// <summary>
        /// Executes the first attempt.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="attempt">The attempt.</param>
        /// <param name="transactionNumber">The transaction number.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The result.</returns>
        Task<TResult> ExecuteAttemptAsync(RetryableWriteOperationContext context, int attempt, long? transactionNumber, CancellationToken cancellationToken);
    }
}
