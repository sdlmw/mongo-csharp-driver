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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver.Core.Bindings;

namespace MongoDB.Driver.Core.Operations
{
    internal static class RetryableWriteOperationExecutor
    {
        // public static methods
        public static TResult Execute<TResult>(IRetryableWriteOperation<TResult> operation, RetryableWriteContext context, CancellationToken cancellationToken)
        {
            if (context.Channel == null)
            {
                context.SetChannelSource(context.Binding.GetWriteChannelSource(cancellationToken));
                context.SetChannel(context.ChannelSource.GetChannel(cancellationToken));
            }

            if (!context.WithRetry || !AreRetryableWritesSupported(context))
            {
                return operation.ExecuteInitialAttempt(context, null, cancellationToken);
            }

            var transactionNumber = context.Binding.Session.AdvanceTransactionId();
            Exception originalException;
            try
            {
                return operation.ExecuteInitialAttempt(context, transactionNumber, cancellationToken);
            }
            catch (Exception ex) when (IsRetryableException(ex))
            {
                originalException = ex;
            }

            try
            {
                context.SetChannelSource(context.Binding.GetWriteChannelSource(cancellationToken));
                context.SetChannel(context.ChannelSource.GetChannel(cancellationToken));
            }
            catch
            {
                throw originalException;
            }

            if (!AreRetryableWritesSupported(context))
            {
                throw originalException;
            }

            try
            {
                return operation.ExecuteRetry(context, transactionNumber, cancellationToken);
            }
            catch (Exception ex) when (ShouldThrowOriginalException(ex))
            {
                throw originalException;
            }
        }

        public static Task<TResult> ExecuteAsync<TResult>(IRetryableWriteOperation<TResult> operation, RetryableWriteContext context, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        // privates static methods
        private static bool AreRetryableWritesSupported(RetryableWriteContext context)
        {
            throw new NotImplementedException();
        }

        private static bool IsRetryableException(Exception ex)
        {
            throw new NotImplementedException();
        }

        private static bool ShouldThrowOriginalException(Exception ex)
        { 
            throw new NotImplementedException();
        }
    }
}
