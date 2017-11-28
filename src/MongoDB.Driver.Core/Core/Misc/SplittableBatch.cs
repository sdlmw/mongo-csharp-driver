/* Copyright 2017 MongoDB Inc.
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

namespace MongoDB.Driver.Core.Misc
{
    /// <summary>
    /// Represens a batch of items that can be split into two batches if necessary.
    /// </summary>
    /// <typeparam name="TItem">The type of the items.</typeparam>
    public class SplittableBatch<TItem>
    {
        // private fields
        private readonly bool _canBeSplit;
        private readonly ArraySegment<TItem> _items;
        private int _splitIndex = -1;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="SplittableBatch{TItem}" /> class.
        /// </summary>
        /// <param name="items">The items.</param>
        /// <param name="canBeSplit">if set to <c>true</c> the batch can be split.</param>
        public SplittableBatch(ArraySegment<TItem> items, bool  canBeSplit = true)
        {
            _items = items;
            _canBeSplit = canBeSplit;
        }

        // public properties
        /// <summary>
        /// Gets a value indicating whether this instance can be split.
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance can be split; otherwise, <c>false</c>.
        /// </value>
        public bool CanBeSplit => _canBeSplit;
        /// <summary>
        /// Gets the items.
        /// </summary>
        /// <value>
        /// The items.
        /// </value>
        public ArraySegment<TItem> Items => _items;

        /// <summary>
        /// Gets the index of the split.
        /// </summary>
        /// <value>
        /// The index of the split.
        /// </value>
        public int SplitIndex => _splitIndex;

        // public method
        /// <summary>
        /// Splits the batch at index.
        /// </summary>
        /// <param name="index">The index to split at.</param>
        public void SplitAt(int index)
        {
            Ensure.IsBetween(index, 0, _items.Count, nameof(index));
            if (_canBeSplit)
            {
                _splitIndex = index;
            }
            else
            {
                throw new InvalidOperationException("Batch cannot be split.");
            }
        }
    }
}
