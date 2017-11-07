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

using System.Reflection;

namespace MongoDB.Driver.Tests
{
    internal static class WrappingClientSessionReflector
    {
        public static bool IsDisposed(this WrappingClientSession obj)
        {
            var methodInfo = typeof(WrappingClientSession).GetMethod("IsDisposed", BindingFlags.NonPublic | BindingFlags.Instance);
            return (bool)methodInfo.Invoke(obj, new object[0]);
        }
    }
}
