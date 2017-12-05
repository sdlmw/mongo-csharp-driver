/* Copyright 2013-2017 MongoDB Inc.
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
using FluentAssertions;
using MongoDB.Bson.TestHelpers.XunitExtensions;
using Xunit;

namespace MongoDB.Driver.Core.Misc
{
    public class BatchableSourceTests
    {
        [Theory]
        [ParameterAttributeData]
        public void ToList_should_return_the_expected_result(
            [Values(0, 1, 2, 3)] int length)
        {
            var list = new List<int>();
            for (var i = 0; i < length; i++)
            {
                list.Add(i);
            }
            var enumerator = list.GetEnumerator();

            var result = BatchableSourceReflector.ToList(enumerator);

            result.Should().Equal(list);
        }

        [Theory]
        [ParameterAttributeData]
        public void constructor_with_enumerable_should_initialize_instance(
            [Values(0, 1, 2, 3)] int length)
        {
            var list = new List<int>();
            for (var i = 0; i < length; i++)
            {
                list.Add(i);
            }
            var enumerable = (IEnumerable<int>)list;

            var result = new BatchableSource<int>(enumerable);

            result.AdjustedCount.Should().Be(length);
            result.CanBeAdjusted.Should().BeTrue();
            result.Count.Should().Be(length);
            result.Items.Should().Equal(list);
            result.Offset.Should().Be(0);
        }

        [Fact]
        public void constructor_with_enumerable_should_throw_when_enumerable_is_null()
        {
            var exception = Record.Exception(() => new BatchableSource<int>((IEnumerable<int>)null));

            var e = exception.Should().BeOfType<ArgumentNullException>().Subject;
            e.ParamName.Should().Be("batch");
        }

        [Theory]
        [ParameterAttributeData]
        public void constructor_with_enumerator_should_initialize_instance(
            [Values(0, 1, 2, 3)] int length)
        {
            var list = new List<int>();
            for (var i = 0; i < length; i++)
            {
                list.Add(i);
            }
            var enumerator = list.GetEnumerator(); ;

            var result = new BatchableSource<int>(enumerator);

            result.AdjustedCount.Should().Be(length);
            result.CanBeAdjusted.Should().BeTrue();
            result.Count.Should().Be(length);
            result.Items.Should().Equal(list);
            result.Offset.Should().Be(0);
        }

        [Fact]
        public void constructor_with_enumerator_should_throw_when_enumerable_is_null()
        {
            var exception = Record.Exception(() => new BatchableSource<int>((IEnumerator<int>)null));

            var e = exception.Should().BeOfType<ArgumentNullException>().Subject;
            e.ParamName.Should().Be("enumerator");
        }

        [Theory]
        [ParameterAttributeData]
        public void constructor_with_list_should_initialize_instance(
            [Values(0, 1, 2, 3)] int length)
        {
            var list = new List<int>();
            for (var i = 0; i < length; i++)
            {
                list.Add(i);
            }

            var result = new BatchableSource<int>(list);

            result.AdjustedCount.Should().Be(length);
            result.CanBeAdjusted.Should().BeFalse();
            result.Count.Should().Be(length);
            result.Items.Should().Equal(list);
            result.Offset.Should().Be(0);
        }

        [Fact]
        public void constructor_with_list_should_throw_when_list_is_null()
        {
            var exception = Record.Exception(() => new BatchableSource<int>((IReadOnlyList<int>)null));

            var e = exception.Should().BeOfType<ArgumentNullException>().Subject;
            e.ParamName.Should().Be("items");
        }

        [Theory]
        [ParameterAttributeData]
        public void constructor_with_list_and_canBeAdjusted_should_initialize_instance(
            [Values(0, 1, 2, 3)] int length,
            [Values(false, true)] bool canBeAdjusted)
        {
            var list = new List<int>();
            for (var i = 0; i < length; i++)
            {
                list.Add(i);
            }

            var result = new BatchableSource<int>(list, canBeAdjusted);

            result.AdjustedCount.Should().Be(length);
            result.CanBeAdjusted.Should().Be(canBeAdjusted);
            result.Count.Should().Be(length);
            result.Items.Should().Equal(list);
            result.Offset.Should().Be(0);
        }

        [Fact]
        public void constructor_with_list_and_canBeAdjusted_should_throw_when_list_is_null()
        {
            var exception = Record.Exception(() => new BatchableSource<int>((IReadOnlyList<int>)null, true));

            var e = exception.Should().BeOfType<ArgumentNullException>().Subject;
            e.ParamName.Should().Be("items");
        }

        [Theory]
        [ParameterAttributeData]
        public void constructor_with_list_offset_count_and_canBeAdjusted_should_initialize_instance(
            [Values(3, 4)] int length,
            [Values(0, 1)] int offset,
            [Values(1, 2)] int count,
            [Values(false, true)] bool canBeAdjusted)
        {
            var list = new List<int>();
            for (var i = 0; i < length; i++)
            {
                list.Add(i);
            }

            var result = new BatchableSource<int>(list, offset, count, canBeAdjusted);

            result.AdjustedCount.Should().Be(count);
            result.CanBeAdjusted.Should().Be(canBeAdjusted);
            result.Count.Should().Be(count);
            result.Items.Should().Equal(list);
            result.Offset.Should().Be(offset);
        }

        [Fact]
        public void constructor_with_list_offset_count_and_canBeAdjusted_should_throw_when_list_is_null()
        {
            var exception = Record.Exception(() => new BatchableSource<int>((IReadOnlyList<int>)null, 0, 0, true));

            var e = exception.Should().BeOfType<ArgumentNullException>().Subject;
            e.ParamName.Should().Be("items");
        }

        [Theory]
        [InlineData(0, -1)]
        [InlineData(0, 1)]
        [InlineData(1, -1)]
        [InlineData(1, 2)]
        [InlineData(2, -1)]
        [InlineData(2, 3)]
        public void constructor_with_list_offset_count_and_canBeAdjusted_should_throw_when_offset_is_invalid(int length, int offset)
        {
            var list = Enumerable.Range(0, length).ToList();

            var exception = Record.Exception(() => new BatchableSource<int>(list, offset, 0, true));

            var e = exception.Should().BeOfType<ArgumentOutOfRangeException>().Subject;
            e.ParamName.Should().Be("offset");
        }
        [Theory]
        [InlineData(0, 0, -1)]
        [InlineData(0, 0, 1)]
        [InlineData(1, 0, -1)]
        [InlineData(1, 0, 2)]
        [InlineData(1, 1, 1)]
        [InlineData(2, 0, -1)]
        [InlineData(2, 0, 3)]
        [InlineData(2, 1, -1)]
        [InlineData(2, 1, 2)]
        [InlineData(2, 2, -1)]
        [InlineData(2, 2, 1)]
        public void constructor_with_list_offset_count_and_canBeAdjusted_should_throw_when_count_is_invalid(int length, int offset, int count)
        {
            var list = Enumerable.Range(0, length).ToList();

            var exception = Record.Exception(() => new BatchableSource<int>(list, offset, count, true));

            var e = exception.Should().BeOfType<ArgumentOutOfRangeException>().Subject;
            e.ParamName.Should().Be("count");
        }

        [Theory]
        [ParameterAttributeData]
        public void AdjustedCount_should_return_expected_result(
            [Values(0, 1, 2, 3)] int value)
        {
            var subject = CreateSubject();
            subject.SetAdjustedCount(value);

            var result = subject.AdjustedCount;

            result.Should().Be(value);
        }

        [Theory]
        [ParameterAttributeData]
        public void CanBeAdjusted_should_return_expected_result(
            [Values(false, true)] bool value)
        {
            var subject = CreateSubject(canBeAdjusted: value);

            var result = subject.CanBeAdjusted;

            result.Should().Be(value);
        }

        [Theory]
        [ParameterAttributeData]
        public void Count_should_return_expected_result(
            [Values(0, 1, 2, 3)] int value)
        {
            var subject = CreateSubject(count: value);

            var result = subject.Count;

            result.Should().Be(value);
        }

        [Theory]
        [ParameterAttributeData]
        public void Items_should_return_expected_result(
            [Values(0, 1, 2, 3)] int length)
        {
            var subject = CreateSubject(length: length);

            var result = subject.Items;

            result.Should().Equal(Enumerable.Range(0, length));
        }

        [Theory]
        [ParameterAttributeData]
        public void Offset_should_return_expected_result(
            [Values(0, 1, 2, 3)] int value)
        {
            var subject = CreateSubject(length: 4, offset: value, count: 1);

            var result = subject.Offset;

            result.Should().Be(value);
        }

        [Theory]
        [InlineData(0, 0, 0, 0, 0, 0)]
        [InlineData(1, 0, 0, 0, 0, 0)]
        [InlineData(1, 0, 1, 0, 0, 1)]
        [InlineData(1, 0, 1, 1, 1, 0)]
        [InlineData(1, 1, 0, 0, 1, 0)]
        [InlineData(2, 0, 0, 0, 0, 0)]
        [InlineData(2, 0, 1, 0, 0, 1)]
        [InlineData(2, 0, 1, 1, 1, 0)]
        [InlineData(2, 0, 2, 0, 0, 2)]
        [InlineData(2, 0, 2, 1, 1, 1)]
        [InlineData(2, 0, 2, 2, 2, 0)]
        [InlineData(2, 1, 0, 0, 1, 0)]
        [InlineData(2, 1, 1, 0, 1, 1)]
        [InlineData(2, 1, 1, 1, 2, 0)]
        [InlineData(2, 2, 0, 0, 2, 0)]
        public void AdvanceOverAdjustedBatch_should_have_expected_result(int length, int offset, int count, int adjustedCount, int expectedOffset, int expectedCount)
        {
            var subject = CreateSubject(length: length, offset: offset, count: count, canBeAdjusted: true);
            subject.SetAdjustedCount(adjustedCount);

            subject.AdvanceOverAdjustedBatch();

            subject.AdjustedCount.Should().Be(expectedCount);
            subject.Count.Should().Be(expectedCount);
            subject.Offset.Should().Be(expectedOffset);
        }

        [Fact]
        public void AdvanceOverAdjustedBatch_should_throw_when_canBeAdjusted_is_false()
        {
            var subject = CreateSubject(canBeAdjusted: false);

            var exception = Record.Exception(() => subject.AdvanceOverAdjustedBatch());

            exception.Should().BeOfType<InvalidOperationException>();
        }

        [Theory]
        [InlineData(0, 0, 0, 0, new int[] { })]
        [InlineData(1, 0, 0, 0, new int[] { })]
        [InlineData(1, 0, 1, 0, new int[] { })]
        [InlineData(1, 0, 1, 1, new int[] { 0 })]
        [InlineData(1, 1, 0, 0, new int[] { })]
        [InlineData(2, 0, 0, 0, new int[] { })]
        [InlineData(2, 0, 1, 0, new int[] { })]
        [InlineData(2, 0, 1, 1, new int[] { 0 })]
        [InlineData(2, 0, 2, 0, new int[] { })]
        [InlineData(2, 0, 2, 1, new int[] { 0 })]
        [InlineData(2, 0, 2, 2, new int[] { 0, 1 })]
        [InlineData(2, 1, 0, 0, new int[] { })]
        [InlineData(2, 1, 1, 0, new int[] { })]
        [InlineData(2, 1, 1, 1, new int[] { 1 })]
        [InlineData(2, 2, 0, 0, new int[] { })]
        public void GetItemsInAdjustedBatch_should_return_expected_result(int length, int offset, int count, int adjustedCount, int[] expectedResult)
        {
            var subject = CreateSubject(length: length, offset: offset, count: count);
            subject.SetAdjustedCount(adjustedCount);

            var result = subject.GetItemsInAdjustedBatch();

            result.Should().Equal(expectedResult);
        }

        [Theory]
        [InlineData(0, 0, 0, 0)]
        [InlineData(1, 0, 0, 0)]
        [InlineData(1, 0, 1, 0)]
        [InlineData(1, 0, 1, 1)]
        [InlineData(1, 1, 0, 0)]
        [InlineData(2, 0, 0, 0)]
        [InlineData(2, 1, 0, 0)]
        [InlineData(2, 1, 1, 0)]
        [InlineData(2, 1, 1, 1)]
        [InlineData(2, 2, 0, 0)]
        public void SetAdjustedCount_should_have_expected_result(int length, int offset, int count, int value)
        {
            var subject = CreateSubject(length: length, offset: offset, count: count);

            subject.SetAdjustedCount(value);

            subject.AdjustedCount.Should().Be(value);
        }

        [Fact]
        public void SetAdjustedCount_should_throw_when_canBeAdjusted_is_false()
        {
            var subject = CreateSubject(canBeAdjusted: false);

            var exception = Record.Exception(() => subject.SetAdjustedCount(0));

            exception.Should().BeOfType<InvalidOperationException>();
        }

        [Theory]
        [InlineData(0, 0, -1)]
        [InlineData(0, 0, 1)]
        [InlineData(1, 0, -1)]
        [InlineData(1, 0, 1)]
        [InlineData(1, 1, -1)]
        [InlineData(1, 1, 2)]
        [InlineData(2, 0, -1)]
        [InlineData(2, 0, 1)]
        [InlineData(2, 1, -1)]
        [InlineData(2, 1, 2)]
        [InlineData(2, 2, -1)]
        [InlineData(2, 2, 3)]
        public void SetAdjustedCount_should_throw_when_value_is_invalid(int length, int count, int value)
        {
            var subject = CreateSubject(length: length, count: count);

            var exception = Record.Exception(() => subject.SetAdjustedCount(value));

            var e = exception.Should().BeOfType<ArgumentOutOfRangeException>().Subject;
            e.ParamName.Should().Be("value");
        }

        // private methods
        private BatchableSource<int> CreateSubject(
            int? length = null,
            int? offset = null,
            int? count = null,
            bool canBeAdjusted = true)
        {
            var list = Enumerable.Range(0, length ?? 3).ToList();
            offset = offset ?? 0;
            count = count ?? list.Count;
            return new BatchableSource<int>(list, offset.Value, count.Value, canBeAdjusted);
        }
    }

    internal class BatchableSourceReflector
    {
        public static List<T> ToList<T>(IEnumerator<T> enumerator)
        {
            var methodInfo = typeof(BatchableSource<T>).GetMethod("ToList", BindingFlags.NonPublic | BindingFlags.Static);
            return (List<T>)methodInfo.Invoke(null, new object[] { enumerator });
        }
    }
}