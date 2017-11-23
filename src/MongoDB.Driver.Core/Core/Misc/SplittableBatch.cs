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
using System.Collections.Generic;
using System.Linq;

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
        private readonly List<TItem> _items;
        private int _splitIndex = -1;

        // constructors
        /// <summary>
        /// Initializes a new instance of the <see cref="SplittableBatch{TItem}" /> class.
        /// </summary>
        /// <param name="items">The items.</param>
        /// <param name="canBeSplit">if set to <c>true</c> the batch can be split.</param>
        public SplittableBatch(List<TItem> items, bool  canBeSplit = true)
        {
            _items = Ensure.IsNotNull(items, nameof(items));
            _canBeSplit = canBeSplit;
        }

        // public properties
        /// <summary>
        /// Gets the items.
        /// </summary>
        /// <value>
        /// The items.
        /// </value>
        public List<TItem> Items => _items;

        /// <summary>
        /// Gets a value indicating whether the batch was split.
        /// </summary>
        /// <value>
        ///   <c>true</c> if the batch was split; otherwise, <c>false</c>.
        /// </value>
        public bool WasSplit => _splitIndex != -1;

        // public method
        /// <summary>
        /// Gets the first batch.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException">GetFirstBatch is not valid when canBeSplit is false.</exception>
        public SplittableBatch<TItem> GetSplit1()
        {
            if (_splitIndex == -1)
            {
                throw new InvalidOperationException("Split must be called first.");
            }

            return new SplittableBatch<TItem>(_items.Take(_splitIndex).ToList(), canBeSplit: false);
        }

        /// <summary>
        /// Gets the second batch.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException">GetFirstBatch is not valid when canBeSplit is false.</exception>
        public SplittableBatch<TItem> GetSplit2()
        {
            if (_splitIndex == -1)
            {
                throw new InvalidOperationException("Split must be called first.");
            }

            return new SplittableBatch<TItem>(_items.Skip(_splitIndex).ToList(), canBeSplit: true);
        }

        /// <summary>
        /// Splits the batch at index;
        /// </summary>
        /// <param name="index">The index to split at.</param>
        /// <exception cref="InvalidOperationException">SplitAt cannot be called when canBeSplit is false.</exception>
        public void Split(int index)
        {
            Ensure.IsBetween(index, 0, _items.Count, nameof(index));
            if (_canBeSplit)
            {
                _splitIndex = index;
            }
            else
            {
                throw new InvalidOperationException("SplitAt cannot be called when canBeSplit is false.");
            }
        }
    }
}
