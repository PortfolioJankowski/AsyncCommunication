using Microsoft.Extensions.Caching.Distributed;

namespace BuildingBlocks.Db
{
    public class CachedDbRepositoryDecorator(IDbRepository innerRepository,
            IDistributedCache cache) : IDbRepository
    {
        public byte[] GetDataBlob(Guid id)
        {
            var cachedValue = cache.GetString(id.ToString());
            if (cachedValue != null)
            {
                return Convert.FromBase64String(cachedValue);
            }

            return innerRepository.GetDataBlob(id);
        }

        public Guid GetFromOutbox()
        {
            return innerRepository.GetFromOutbox();
        }

        public Guid InsertData(byte[] blob)
        {
            var recordId = innerRepository.InsertData(blob);

            // 2. Dodaj do cache (Redis) po udanym zapisie do DB/Outbox
            // Używamy Base64, ponieważ IDistributedCache pracuje najlepiej z ciągami znaków/byte[]
            // W tym przypadku Convert.ToBase64String(blob) działa jako bezpieczny ciąg znaków.
            cache.SetString(
                recordId.ToString(),
                Convert.ToBase64String(blob),
                new DistributedCacheEntryOptions
                {
                    AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5) // Ustawiamy TTL (Redis)
                });

            return recordId;
        }
    }
}
