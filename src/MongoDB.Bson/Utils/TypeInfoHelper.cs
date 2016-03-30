/* Copyright 2016 MongoDB Inc.
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

namespace MongoDB.Bson.Utils
{
    internal static class TypeInfoHelper
    {
        // public static methods
        public static IEnumerable<MemberInfo> GetAllMembers(TypeInfo typeInfo)
        {
            var declaringTypeInfos = new Stack<TypeInfo>();
            var declaringType = typeInfo.AsType();
            while (declaringType != null)
            {
                var declaringTypeInfo = declaringType.GetTypeInfo();
                declaringTypeInfos.Push(declaringTypeInfo);
                declaringType = declaringTypeInfo.BaseType;
            }

            foreach (var declaringTypeInfo in declaringTypeInfos)
            {
                foreach (var memberInfo in declaringTypeInfo.DeclaredMembers)
                {
                    yield return memberInfo;
                }
            }
        }

        public static ConstructorInfo GetMatchingConstructor(TypeInfo typeInfo, IEnumerable<Type> argumentTypes)
        {
            return typeInfo.DeclaredConstructors
                .Where(c => c.GetParameters().Select(p => p.ParameterType).SequenceEqual(argumentTypes))
                .SingleOrDefault();
        }

        public static IEnumerable<MemberInfo> GetMatchingMembers(TypeInfo typeInfo, string name, MemberTypes memberTypes, BindingFlags bindingFlags)
        {
            var stringComparison = (bindingFlags & BindingFlags.IgnoreCase) != 0 ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
            return TypeInfoHelper.GetAllMembers(typeInfo)
                .Where(
                    m => m.Name.Equals(name, stringComparison) &&
                    TypeInfoHelper.MatchesMemberTypes(m, memberTypes) &&
                    TypeInfoHelper.MatchesBindingFlags(m, bindingFlags));
        }

        public static bool IsPublic(MemberInfo memberInfo)
        {
            FieldInfo fieldInfo;
            if ((fieldInfo = memberInfo as FieldInfo) != null)
            {
                return fieldInfo.IsPublic;
            }

            PropertyInfo propertyInfo;
            if ((propertyInfo = memberInfo as PropertyInfo) != null)
            {
                return propertyInfo.GetAccessors().All(a => a.IsPublic);
            }

            MethodBase methodBase;
            if ((methodBase = memberInfo as MethodBase) != null)
            {
                return methodBase.IsPublic;
            }

            return false;
        }

        public static bool IsStatic(MemberInfo memberInfo)
        {
            FieldInfo fieldInfo;
            if ((fieldInfo = memberInfo as FieldInfo) != null)
            {
                return fieldInfo.IsStatic;
            }

            PropertyInfo propertyInfo;
            if ((propertyInfo = memberInfo as PropertyInfo) != null)
            {
                return (propertyInfo.GetMethod ?? propertyInfo.SetMethod).IsStatic;
            }

            MethodBase methodBase;
            if ((methodBase = memberInfo as MethodBase) != null)
            {
                return methodBase.IsStatic;
            }

            return false;
        }

        // private static methods
        private static bool MatchesBindingFlags(MemberInfo memberInfo, BindingFlags bindingFlags)
        {
            return
                ((bindingFlags & BindingFlags.Instance) == 0 || !IsStatic(memberInfo)) &&
                ((bindingFlags & BindingFlags.Static) == 0 || IsStatic(memberInfo)) &&
                ((bindingFlags & BindingFlags.Public) == 0 || IsPublic(memberInfo)) &&
                ((bindingFlags & BindingFlags.NonPublic) == 0) || !IsPublic(memberInfo);
        }

        private static bool MatchesMemberTypes(MemberInfo memberInfo, MemberTypes memberTypes)
        {
            return
                (memberTypes & MemberTypes.Constructor) != 0 && memberInfo is ConstructorInfo ||
                (memberTypes & MemberTypes.Field) != 0 && memberInfo is FieldInfo ||
                (memberTypes & MemberTypes.Method) != 0 && memberInfo is MethodInfo ||
                (memberTypes & MemberTypes.Property) != 0 && memberInfo is PropertyInfo;
        }
    }
}
