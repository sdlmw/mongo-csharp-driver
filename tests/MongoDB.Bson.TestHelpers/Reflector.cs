﻿/* Copyright 2018-present MongoDB Inc.
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

using System.Linq;
using System.Reflection;

namespace MongoDB.Bson.TestHelpers
{
    public static class Reflector
    {
        public static object GetFieldValue(object obj, string name)
        {
            var fieldInfo = obj.GetType().GetField(name, BindingFlags.NonPublic | BindingFlags.Instance);
            return fieldInfo.GetValue(obj);
        }

        public static object Invoke(object obj, string name)
        {
            var methodInfo = obj.GetType().GetMethods(BindingFlags.NonPublic | BindingFlags.Instance)
                .Where(m => m.Name == name && m.GetParameters().Length == 0)
                .Single();
            return methodInfo.Invoke(obj, new object[] { });
        }
    }
}
