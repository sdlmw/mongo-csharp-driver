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

namespace MongoDB.Bson.IO.JsonConverters
{
    /// <summary>
    /// Represents a converter from Decimal128 to shell JSON.
    /// </summary>
    public class Decimal128ShellJsonConverter : IJsonConverter<Decimal128>
    {
        /// <inheritdoc/>
        public void Convert(Decimal128 value, IStrictJsonWriter writer)
        {
            var representation = $"NumberDecimal(\"{value.ToString()}\")";
            writer.WriteValue(representation);
        }
    }
}
