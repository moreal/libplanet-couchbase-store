using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.Transactions;
using Libplanet.Store.Trie;

namespace Libplanet.CouchbaseStore
{
    public class CouchbaseKeyValueStore : IKeyValueStore
    {
        private readonly IBucket _bucket;
        private readonly Transactions _transactions;

        public CouchbaseKeyValueStore(IBucket bucket)
        {
            _bucket = bucket;
            _transactions = Transactions.Create(_bucket.Cluster);
        }

        public void Dispose()
        {
            _bucket.Dispose();
        }

        public byte[] Get(in KeyBytes key)
        {
            return _bucket.DefaultCollection().GetAsync(key.Hex).Result.ContentAs<byte[]>();
        }

        public IReadOnlyDictionary<KeyBytes, byte[]> Get(IEnumerable<KeyBytes> keys)
        {
            var keysArray = keys.ToArray();
            var collection = _bucket.DefaultCollection();
            var results = Task.WhenAll(keysArray.Select(key => collection.GetAsync(key.Hex)).ToArray()).Result;
            return keysArray.Zip(results.Select(result => result.ContentAs<byte[]>()), ValueTuple.Create)
                .ToDictionary(t => t.Item1, t => t.Item2);
        }

        public void Set(in KeyBytes key, byte[] value)
        {
            _bucket.DefaultCollection().InsertAsync(key.Hex, value).Wait();
        }

        public void Set(IDictionary<KeyBytes, byte[]> values)
        {
            _transactions.RunAsync(async ctx =>
            {
                foreach (var pair in values)
                {
                    await ctx.InsertAsync(await _bucket.DefaultCollectionAsync(), pair.Key.Hex, pair.Value);
                }

                await ctx.CommitAsync();
            }).Wait();
        }

        public void Delete(in KeyBytes key)
        {
            _bucket.DefaultCollection().RemoveAsync(key.Hex).Wait();
        }

        public void Delete(IEnumerable<KeyBytes> keys)
        {
            _transactions.RunAsync(async ctx =>
            {
                var collection = await _bucket.DefaultCollectionAsync();
                foreach (var key in keys)
                {
                    var doc = await ctx.GetAsync(collection, key.Hex);
                    await ctx.RemoveAsync(doc);
                }

                await ctx.CommitAsync();
            }).Wait();
        }

        public bool Exists(in KeyBytes key)
        {
            return _bucket.DefaultCollection().ExistsAsync(key.Hex).Result.Exists;
        }

        public IEnumerable<KeyBytes> ListKeys()
        {
            throw new NotSupportedException();
        }
    }
}
