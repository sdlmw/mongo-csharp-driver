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

namespace System.Reflection
{
    internal static class TypeInfoExtensions
    {
        // public static methods
        public static ConstructorInfo GetConstructor(this TypeInfo typeInfo, Type[] parameterTypes)
        {
            return typeInfo.DeclaredConstructors
                .Where(c =>
                    !c.IsStatic &&
                    c.GetParameters().Select(p => p.ParameterType).SequenceEqual(parameterTypes))
                .SingleOrDefault();

        }

        public static IEnumerable<MemberInfo> GetMember(this TypeInfo typeInfo, string name, MemberTypes memberTypes, BindingFlags bindingFlags)
        {
            var stringComparison = (bindingFlags & BindingFlags.IgnoreCase) != 0 ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
            return typeInfo.GetMembers()
                .Where(m =>
                    m.Name.Equals(name, stringComparison) &&
                    MatchesMemberTypes(m, memberTypes) &&
                    MatchesBindingFlags(m, bindingFlags));
        }

        public static IEnumerable<MemberInfo> GetMembers(this TypeInfo typeInfo, BindingFlags bindingFlags)
        {
            return typeInfo.GetMembers()
                .Where(m => MatchesBindingFlags(m, bindingFlags));
        }

        public static IEnumerable<MemberInfo> GetMembers(this TypeInfo typeInfo)
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

        // private static methods
        private static bool IsPublic(MemberInfo memberInfo)
        {
            FieldInfo fieldInfo;
            if ((fieldInfo = memberInfo as FieldInfo) != null)
            {
                return fieldInfo.IsPublic;
            }

            PropertyInfo propertyInfo;
            if ((propertyInfo = memberInfo as PropertyInfo) != null)
            {
                return propertyInfo.GetAccessors().Any(a => a.IsPublic);
            }

            MethodBase methodBase;
            if ((methodBase = memberInfo as MethodBase) != null)
            {
                return methodBase.IsPublic;
            }

            return false;
        }

        private static bool IsStatic(MemberInfo memberInfo)
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

        private static bool MatchesBindingFlags(MemberInfo memberInfo, BindingFlags bindingFlags)
        {
            if (IsPublic(memberInfo))
            {
                if ((bindingFlags & BindingFlags.Public) == 0)
                {
                    return false;
                }
            }
            else
            {
                if ((bindingFlags & BindingFlags.NonPublic) == 0)
                {
                    return false;
                }
            }

            if (IsStatic(memberInfo))
            {
                if ((bindingFlags & BindingFlags.Static) == 0)
                {
                    return false;
                }
            }
            else
            {
                if ((bindingFlags & BindingFlags.Instance) == 0)
                {
                    return false;
                }
            }

            return true;
        }

        private static bool MatchesMemberTypes(MemberInfo memberInfo, MemberTypes memberTypes)
        {
            if (memberInfo is ConstructorInfo)
            {
                return (memberTypes & MemberTypes.Constructor) != 0;
            }

            if (memberInfo is FieldInfo)
            {
                return (memberTypes & MemberTypes.Field) != 0;
            }

            if (memberInfo is MethodInfo)
            {
                return (memberTypes & MemberTypes.Method) != 0;
            }

            if (memberInfo is PropertyInfo)
            {
                return (memberTypes & MemberTypes.Property) != 0;
            }

            throw new NotSupportedException("Unexpected memberInfo type.");
        }
    }
}
