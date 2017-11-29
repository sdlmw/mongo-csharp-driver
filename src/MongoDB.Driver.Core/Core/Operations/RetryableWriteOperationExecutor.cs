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
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver.Core.Bindings;
using MongoDB.Driver.Core.Connections;

namespace MongoDB.Driver.Core.Operations
{
    internal static class RetryableWriteOperationExecutor
    {
        // public static methods
        public static TResult Execute<TResult>(IRetryableWriteOperation<TResult> operation, IWriteBinding binding, bool retryRequested, CancellationToken cancellationToken)
        {
            using (var context = new RetryableWriteOperationContext(binding, retryRequested))
            {
                return Execute(operation, context, cancellationToken);
            }
        }

        public static TResult Execute<TResult>(IRetryableWriteOperation<TResult> operation, RetryableWriteOperationContext context, CancellationToken cancellationToken)
        {
            if (context.Channel == null)
            {
                context.SetChannelSource(context.Binding.GetWriteChannelSource(cancellationToken));
                context.SetChannel(context.ChannelSource.GetChannel(cancellationToken));
            }

            if (!context.RetryRequested || !AreRetryableWritesSupported(context.Channel.ConnectionDescription))
            {
                return operation.ExecuteAttempt(context, 1, null, cancellationToken);
            }

            var transactionNumber = context.Binding.Session.AdvanceTransactionNumber();
            Exception originalException;
            try
            {
                return operation.ExecuteAttempt(context, 1, transactionNumber, cancellationToken);
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

            if (!AreRetryableWritesSupported(context.Channel.ConnectionDescription))
            {
                throw originalException;
            }

            try
            {
                return operation.ExecuteAttempt(context, 2, transactionNumber, cancellationToken);
            }
            catch (Exception ex) when (ShouldThrowOriginalException(ex))
            {
                throw originalException;
            }
        }

        public async static Task<TResult> ExecuteAsync<TResult>(IRetryableWriteOperation<TResult> operation, IWriteBinding binding, bool retryRequested, CancellationToken cancellationToken)
        {
            using (var context = new RetryableWriteOperationContext(binding, retryRequested))
            {
                return await ExecuteAsync(operation, context, cancellationToken).ConfigureAwait(false);
            }
        }

        public static async Task<TResult> ExecuteAsync<TResult>(IRetryableWriteOperation<TResult> operation, RetryableWriteOperationContext context, CancellationToken cancellationToken)
        {
            if (context.Channel == null)
            {
                context.SetChannelSource(await context.Binding.GetWriteChannelSourceAsync(cancellationToken).ConfigureAwait(false));
                context.SetChannel(await context.ChannelSource.GetChannelAsync(cancellationToken).ConfigureAwait(false));
            }

            if (!context.RetryRequested || !AreRetryableWritesSupported(context.Channel.ConnectionDescription))
            {
                return await operation.ExecuteAttemptAsync(context, 1, null, cancellationToken).ConfigureAwait(false);
            }

            var transactionNumber = context.Binding.Session.AdvanceTransactionNumber();
            Exception originalException;
            try
            {
                return await operation.ExecuteAttemptAsync(context, 1, transactionNumber, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (IsRetryableException(ex))
            {
                originalException = ex;
            }

            try
            {
                context.SetChannelSource(await context.Binding.GetWriteChannelSourceAsync(cancellationToken).ConfigureAwait(false));
                context.SetChannel(await context.ChannelSource.GetChannelAsync(cancellationToken).ConfigureAwait(false));
            }
            catch
            {
                throw originalException;
            }

            if (!AreRetryableWritesSupported(context.Channel.ConnectionDescription))
            {
                throw originalException;
            }

            try
            {
                return await operation.ExecuteAttemptAsync(context, 2, transactionNumber, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ShouldThrowOriginalException(ex))
            {
                throw originalException;
            }
        }

        // privates static methods
        private static bool AreRetryableWritesSupported(ConnectionDescription connectionDescription)
        {
            return connectionDescription.IsMasterResult.LogicalSessionTimeout != null;
        }

        private static bool IsRetryableException(Exception ex)
        {
            return
                ex is MongoConnectionException ||
                ex is MongoNotPrimaryException ||
                ex is MongoNodeIsRecoveringException;
        }

        private static bool ShouldThrowOriginalException(Exception retryException)
        {
            return retryException is MongoException && !(retryException is MongoConnectionException);
        }
    }
}
