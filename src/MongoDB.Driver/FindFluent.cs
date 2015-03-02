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

using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver.Core.Misc;

namespace MongoDB.Driver
{
    internal class FindFluent<TDocument, TResult> : FindFluentBase<TDocument, TResult>
    {
        // fields
        private readonly IReadOnlyMongoCollection<TDocument> _collection;
        private Filter<TDocument> _filter;
        private readonly FindOptions<TDocument> _options;
        private readonly Projection<TDocument, TResult> _projection;

        // constructors
        public FindFluent(IReadOnlyMongoCollection<TDocument> collection, Filter<TDocument> filter, Projection<TDocument, TResult> projection, FindOptions<TDocument> options)
        {
            _collection = Ensure.IsNotNull(collection, "collection");
            _filter = Ensure.IsNotNull(filter, "filter");
            _projection = Ensure.IsNotNull(projection, "projection");
            _options = Ensure.IsNotNull(options, "options");
        }

        // properties
        public override Filter<TDocument> Filter
        {
            get { return _filter; }
            set { _filter = Ensure.IsNotNull(value, "value"); }
        }

        public override FindOptions<TDocument> Options
        {
            get { return _options; }
        }

        public override Projection<TDocument, TResult> Projection
        {
            get { return _projection; }
        }

        // methods
        public override Task<long> CountAsync(CancellationToken cancellationToken)
        {
            BsonValue hint;
            _options.Modifiers.TryGetValue("$hint", out hint);
            var options = new CountOptions
            {
                Hint = hint,
                Limit = _options.Limit,
                MaxTime = _options.MaxTime,
                Skip = _options.Skip
            };

            return _collection.CountAsync(_filter, options, cancellationToken);
        }

        public override IFindFluent<TDocument, TResult> Limit(int? limit)
        {
            _options.Limit = limit;
            return this;
        }

        public override IFindFluent<TDocument, TNewResult> Project<TNewResult>(Projection<TDocument, TNewResult> projection)
        {
            return new FindFluent<TDocument, TNewResult>(_collection, _filter, projection, _options);
        }

        public override IFindFluent<TDocument, TResult> Skip(int? skip)
        {
            _options.Skip = skip;
            return this;
        }

        public override IFindFluent<TDocument, TResult> Sort(Sort<TDocument> sort)
        {
            _options.Sort = sort;
            return this;
        }

        public override Task<IAsyncCursor<TResult>> ToCursorAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            return _collection.FindAsync(_filter, _projection, _options, cancellationToken);
        }
    }
}