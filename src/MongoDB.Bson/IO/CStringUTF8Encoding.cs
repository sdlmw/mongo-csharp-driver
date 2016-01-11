/* Copyright 2010-2014 MongoDB Inc.
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
using System.Threading.Tasks;

namespace MongoDB.Bson.IO
{
    // see: https://en.wikipedia.org/wiki/UTF-8

    internal class CStringUTF8Encoding : Encoding
    {
        public static readonly CStringUTF8Encoding Instance = new CStringUTF8Encoding();

        public override int GetByteCount(char[] chars, int index, int count)
        {
            var byteCount = 0;
            var charsEnd = index + count;
            while (index < charsEnd)
            {
                var c = chars[index++];
                if (c <= 0x7f)
                {
                    byteCount += 1;
                }
                else if (c <= 0x7ff)
                {
                    byteCount += 2;
                }
                else
                {
                    byteCount += 3;
                }
            }
            return byteCount;
        }

        public override int GetBytes(char[] chars, int charIndex, int charCount, byte[] bytes, int byteIndex)
        {
            var initialByteIndex = byteIndex;
            var charsEnd = charIndex + charCount;
            while (charIndex < charsEnd)
            {
                var c = chars[charIndex++];
                if (c == 0)
                {
                    throw new BsonSerializationException("CString can not contain null bytes.");
                }
                else if (c <= 0x7f)
                {
                    bytes[byteIndex++] = (byte)c;
                }
                else if (c <= 0x7ff)
                {
                    var byte1 = 0xc0 | (c >> 6);
                    var byte2 = 0x80 | (c & 0x3f);
                    bytes[byteIndex++] = (byte)byte1;
                    bytes[byteIndex++] = (byte)byte2;
                }
                else
                {
                    var byte1 = 0xc0 | (c >> 12);
                    var byte2 = 0x80 | ((c >> 6) & 0x3f);
                    var byte3 = 0x80 | (c & 0x3f);
                    bytes[byteIndex++] = (byte)byte1;
                    bytes[byteIndex++] = (byte)byte2;
                    bytes[byteIndex++] = (byte)byte3;
                }
            }
            return byteIndex - initialByteIndex;
        }

        public override int GetCharCount(byte[] bytes, int index, int count)
        {
            throw new NotImplementedException();
        }

        public override int GetChars(byte[] bytes, int byteIndex, int byteCount, char[] chars, int charIndex)
        {
            throw new NotImplementedException();
        }

        public override int GetMaxByteCount(int charCount)
        {
            return charCount * 3;
        }

        public override int GetMaxCharCount(int byteCount)
        {
            return byteCount;
        }
    }
}
