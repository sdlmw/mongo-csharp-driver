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

namespace MongoDB.Driver.Core.Misc
{
    /// <summary>
    /// Represents a source of items that can be broken into batches.
    /// </summary>
    /// <typeparam name="T">The type of the items.</typeparam>
    public sealed class BatchableSource<T>
    {
        #region static
        // private static methods
        private static List<T> ToList(IEnumerator<T> enumerator)
        {
            var list = new List<T>();
            while (enumerator.MoveNext())
            {
                list.Add(enumerator.Current);
            }
            return list;
        }
        #endregion

        // fields
        private int _adjustedCount;
        private readonly bool _canBeAdjusted;
        private int _count;
        private readonly IReadOnlyList<T> _items;
        private int _offset;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="BatchableSource{T}"/> class.
        /// </summary>
        /// <remarks>
        /// Use this overload when you know the batch is small and won't have to be broken up into sub-batches. 
        /// In that case using this overload is simpler than using an enumerator and using the other constructor.
        /// </remarks>
        /// <param name="batch">The single batch.</param>
        public BatchableSource(IEnumerable<T> batch)
            : this(Ensure.IsNotNull(batch, nameof(batch)).ToList(), true)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BatchableSource{T}"/> class.
        /// </summary>
        /// <param name="enumerator">The enumerator that will provide the items for the batch.</param>
        public BatchableSource(IEnumerator<T> enumerator)
            : this(ToList(Ensure.IsNotNull(enumerator, nameof(enumerator))), true)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BatchableSource{T}" /> class.
        /// </summary>
        /// <param name="items">The items.</param>
        /// <param name="canBeAdjusted">if set to <c>true</c> [can be adjusted].</param>
        /// <remarks>
        /// Use this overload when you know the batch is small and won't have to be broken up into sub-batches.
        /// </remarks>
        public BatchableSource(IReadOnlyList<T> items, bool canBeAdjusted = false)
            : this(Ensure.IsNotNull(items, nameof(items)), 0, items.Count, canBeAdjusted)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BatchableSource{T}"/> class.
        /// </summary>
        /// <param name="items">The items.</param>
        /// <param name="offset">The offset.</param>
        /// <param name="count">The count.</param>
        /// <param name="canBeAdjusted">if set to <c>true</c> the batch can be adjusted.</param>
        public BatchableSource(IReadOnlyList<T> items, int offset, int count, bool canBeAdjusted)
        {
            _items = Ensure.IsNotNull(items, nameof(items));
            _offset = Ensure.IsBetween(offset, 0, items.Count, nameof(offset));
            _count = Ensure.IsBetween(count, 0, items.Count - offset, nameof(count));
            _canBeAdjusted = canBeAdjusted;
            _adjustedCount = count;
        }

        // public properties
        /// <summary>
        /// Gets the adjusted count.
        /// </summary>
        /// <value>
        /// The adjusted count.
        /// </value>
        public int AdjustedCount => _adjustedCount;

        /// <summary>
        /// Gets the most recent batch.
        /// </summary>
        /// <value>
        /// The most recent batch.
        /// </value>
        public IReadOnlyList<T> Batch
        {
            get { return _items.Skip(_offset).Take(_adjustedCount).ToList(); }
        }

        /// <summary>
        /// Gets a value indicating whether the batch can be adjusted.
        /// </summary>
        /// <value>
        ///   <c>true</c> if the batch can be adjusted; otherwise, <c>false</c>.
        /// </value>
        public bool CanBeAdjusted => _canBeAdjusted;

        /// <summary>
        /// Gets the count.
        /// </summary>
        /// <value>
        /// The count.
        /// </value>
        public int Count
        {
            get { return _count; }
        }

        /// <summary>
        /// Gets a value indicating whether there are more items.
        /// </summary>
        /// <value>
        ///   <c>true</c> if there are more items; otherwise, <c>false</c>.
        /// </value>
        public bool HasMore
        {
            get
            {
                return _count > 0;
            }
        }

        /// <summary>
        /// Gets the items.
        /// </summary>
        /// <value>
        /// The items.
        /// </value>
        public IReadOnlyList<T> Items => _items;

        /// <summary>
        /// Gets the offset.
        /// </summary>
        /// <value>
        /// The offset.
        /// </value>
        public int Offset => _offset;

        // public methods
        /// <summary>
        /// Advances the offset.
        /// </summary>
        public void AdvanceOffset()
        {
            if (_canBeAdjusted)
            {
                _offset = _offset + _adjustedCount;
                _count = _count - _adjustedCount;
                _adjustedCount = _count;
            }
            else
            {
                throw new InvalidOperationException("The batch cannot be adjusted.");
            }
        }

        /// <summary>
        /// Sets the adjustedcount.
        /// </summary>
        /// <param name="value">The value.</param>
        public void SetAdjustedCount(int value)
        {
            if (_canBeAdjusted)
            {
                Ensure.IsBetween(value, 0, _count, nameof(value));
                _adjustedCount = value;
            }
            else
            {
                throw new InvalidOperationException("The batch cannot be adjusted.");
            }
        }
    }
}
