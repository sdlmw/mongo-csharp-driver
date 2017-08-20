/* Copyright 2010-2016 MongoDB Inc.
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
using System.IO;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.Events.Diagnostics;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization.Serializers;

namespace MongoDB.Driver.TestConsoleApplication
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new MongoClient();
            var database = client.GetDatabase("test");
            var collection = database.GetCollection<BsonDocument>("test");
            database.DropCollection("test");

            var changeSerializer = new ChangeStreamOutputSerializer<BsonDocument>(BsonDocumentSerializer.Instance, null);
            foreach (var change in collection.Watch().ToEnumerable())
            {
                var writerSettings = new JsonWriterSettings { Indent = true };
                Console.WriteLine(change.RawDocument.ToJson(writerSettings));
            }
        }
    }
}