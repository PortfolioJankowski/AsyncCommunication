using Microsoft.Extensions.Caching.Distributed;

namespace BuildingBlocks.Db
{
    public interface IDbRepository
    {
        Guid InsertData(byte[] blob);
        Guid GetFromOutbox();
        byte[] GetDataBlob(Guid id);
    }
}
