using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization;
using Xunit;

namespace MongoDB.Bson.Tests.Serialization
{
    public class ReadOnlyDictionarySerializerTests
    {
        // container classes for various dictionaries

        public class IrodBox
        {
            public IReadOnlyDictionary<object, object> Irod;
        }

        public class RodBox
        {
            public ReadOnlyDictionary<object, object> Rod;
        }

        public class RodSubclassBox
        {
            public RodSubclass<object, object> RodSub;
        }

        public class CustomIrodBox
        {
            public CustomIrodImplementation<object, object> CustomIrod;
        }

        // "user" created readonly dictionaries

        public class RodSubclass<TKey, TValue> : ReadOnlyDictionary<TKey, TValue>
        {
            public RodSubclass(IDictionary<TKey, TValue> m) : base(m)
            {
            }
        }

        public class CustomIrodImplementation<TKey, TValue> : IReadOnlyDictionary<TKey, TValue>
        {
            private ReadOnlyDictionary<TKey, TValue> _map;

            public CustomIrodImplementation(IDictionary<TKey, TValue> map)
            {
                _map = new ReadOnlyDictionary<TKey, TValue>(map);
            }

            public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => _map.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable) _map).GetEnumerator();

            public int Count => _map.Count;

            public bool ContainsKey(TKey key) => _map.ContainsKey(key);

            public bool TryGetValue(TKey key, out TValue value) => _map.TryGetValue(key, out value);

            public TValue this[TKey key] => _map[key];

            public IEnumerable<TKey> Keys => _map.Keys;
            public IEnumerable<TValue> Values => _map.Values;
        }

        // Tests where nominal type is IReadOnlyDictionary

        [Fact]
        public void TestNominalTypeIReadOnlyDictionaryActualTypeReadOnlyDictionary()
        {
            var map = new Dictionary<object, object> { { "A", 42 } };
            var obj = new IrodBox { Irod = new ReadOnlyDictionary<object, object>(map) };
            var json = obj.ToJson();
            var rep = "{ 'A' : 42 }";
            var expected = "{ 'Irod' : #R }".Replace("#R", rep).Replace("'", "\"");
            Assert.Equal(expected, json);

            var bson = obj.ToBson();
            var rehydrated = BsonSerializer.Deserialize<IrodBox>(bson);
            Assert.IsType<ReadOnlyDictionary<object, object>>(rehydrated.Irod);
            Assert.True(bson.SequenceEqual(rehydrated.ToBson()));
        }

        [Fact]
        public void TestNominalTypeIReadOnlyDictionaryActualTypeCustomIReadOnlyDictionary()
        {
            var map = new Dictionary<object, object> { { "A", 42 } };
            var obj = new IrodBox { Irod = new CustomIrodImplementation<object, object>(map) };
            var json = obj.ToJson();
            var rep = "{ 'A' : 42 }";
            var expected = "{ 'Irod' : #R }".Replace("#R", rep).Replace("'", "\"");
            Assert.Equal(expected, json);

            var bson = obj.ToBson();
            var rehydrated = BsonSerializer.Deserialize<IrodBox>(bson);
            Assert.IsType<ReadOnlyDictionary<object, object>>(rehydrated.Irod);
            Assert.True(bson.SequenceEqual(rehydrated.ToBson()));
        }


        [Fact]
        public void TestNominalTypeIReadOnlyDictionaryActualTypeReadOnlyDictionarySubclass()
        {
            var map = new Dictionary<object, object> { { "A", 42 } };
            var obj = new IrodBox { Irod = new RodSubclass<object, object>(map) };
            var json = obj.ToJson();
            var rep = "{ 'A' : 42 }";
            var expected = "{ 'Irod' : #R }".Replace("#R", rep).Replace("'", "\"");
            Assert.Equal(expected, json);

            var bson = obj.ToBson();
            var rehydrated = BsonSerializer.Deserialize<IrodBox>(bson);
            Assert.IsType<ReadOnlyDictionary<object, object>>(rehydrated.Irod);
            Assert.True(bson.SequenceEqual(rehydrated.ToBson()));
        }

        // Tests where nominal type is ReadOnlyDictionary

        [Fact]
        public void TestNominalTypeReadOnlyDictionaryActualTypeReadOnlyDictionary()
        {
            var map = new Dictionary<object, object> { { "A", 42 } };
            var obj = new RodBox { Rod = new ReadOnlyDictionary<object, object>(map) };
            var json = obj.ToJson();
            var rep = "{ 'A' : 42 }";
            var expected = "{ 'Rod' : #R }".Replace("#R", rep).Replace("'", "\"");
            Assert.Equal(expected, json);

            var bson = obj.ToBson();
            var rehydrated = BsonSerializer.Deserialize<RodBox>(bson);
            Assert.IsType<ReadOnlyDictionary<object, object>>(rehydrated.Rod);
            Assert.True(bson.SequenceEqual(rehydrated.ToBson()));
        }

        [Fact]
        public void TestNominalTypeReadOnlyDictionaryActualTypeReadOnlyDictionarySubclass()
        {
            var map = new Dictionary<object, object> { { "A", 42 } };
            var obj = new RodBox { Rod = new RodSubclass<object, object>(map) };
            var json = obj.ToJson();
            var rep = "{ 'A' : 42 }";
            var expected = "{ 'Rod' : #R }".Replace("#R", rep).Replace("'", "\"");
            Assert.Equal(expected, json);

            var bson = obj.ToBson();
            var rehydrated = BsonSerializer.Deserialize<RodBox>(bson);
            Assert.IsType<ReadOnlyDictionary<object, object>>(rehydrated.Rod);
            Assert.True(bson.SequenceEqual(rehydrated.ToBson()));
        }

        // Tests where nominal type is ReadOnlyDictionary subclass

        [Fact]
        public void TestNominalTypeReadOnlyDictionarySubclassActualTypeReadOnlyDictionarySubclass()
        {
            var map = new Dictionary<object, object> { { "A", 42 } };
            var obj = new RodSubclassBox { RodSub = new RodSubclass<object, object>(map) };
            var json = obj.ToJson();
            var rep = "{ 'A' : 42 }";
            var expected = "{ 'RodSub' : #R }".Replace("#R", rep).Replace("'", "\"");
            Assert.Equal(expected, json);
            var bson = obj.ToBson();
            var rehydrated = BsonSerializer.Deserialize<RodSubclassBox>(bson);
            Assert.IsType<RodSubclass<object, object>>(rehydrated.RodSub);
            Assert.True(bson.SequenceEqual(rehydrated.ToBson()));
        }

        // Tests where nominal type is a custom IReadOnlyDictionary 

        [Fact]
        public void TestNominalTypeCustomIReadOnlyDictionaryActualTypeCustomIReadOnlyDictionary()
        {
            var map = new Dictionary<object, object> { { "A", 42 } };
            var obj = new CustomIrodBox { CustomIrod = new CustomIrodImplementation<object, object>(map) };
            var json = obj.ToJson();
            var rep = "{ 'A' : 42 }";
            var expected = "{ 'CustomIrod' : #R }".Replace("#R", rep).Replace("'", "\"");
            Assert.Equal(expected, json);

            var bson = obj.ToBson();
            var rehydrated = BsonSerializer.Deserialize<CustomIrodBox>(bson);
            Assert.IsType<CustomIrodImplementation<object, object>>(rehydrated.CustomIrod);
            Assert.True(bson.SequenceEqual(rehydrated.ToBson()));
        }

    }
}
