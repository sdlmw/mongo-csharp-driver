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
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MongoDB.Driver.Core.Bindings
{
    public class WrappingCoreSessionTests
    {
    }

    public static class WrappingCoreSessionReflector
    {
        public static bool _disposed(this WrappingCoreSession obj)
        {
            var fieldInfo = typeof(WrappingCoreSession).GetField("_disposed", BindingFlags.NonPublic | BindingFlags.Instance);
            return (bool)fieldInfo.GetValue(obj);
        }

        public static bool _ownsWrapped(this WrappingCoreSession obj)
        {
            var fieldInfo = typeof(WrappingCoreSession).GetField("_ownsWrapped", BindingFlags.NonPublic | BindingFlags.Instance);
            return (bool)fieldInfo.GetValue(obj);
        }
    }
}
