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
    /// Represents a converter from BsonBinaryData to extended JSON.
    /// </summary>
    public class BsonBinaryDataExtendedJsonConverter : IJsonConverter<BsonBinaryData>
    {
        /// <inheritdoc/>
        public void Convert(BsonBinaryData value, IStrictJsonWriter writer)
        {
            writer.WriteStartDocument();
            writer.WriteName("$binary");
            writer.WriteString(System.Convert.ToBase64String(value.Bytes));
            writer.WriteName("$type");
            writer.WriteString(((int)value.SubType).ToString("x2"));
            writer.WriteEndDocument();
        }
    }
}
