namespace BuildingBlocks.Db
{
    public class DbRepository(ApiDbContext dbContext) : IDbRepository
    {
        public byte[] GetDataBlob(Guid id)
        {
            return dbContext.Blobs.FirstOrDefault(b => b.Id == id)?.Data;
        }

        public Guid GetFromOutbox()
        {
            var record = dbContext.Outboxes.FirstOrDefault(o => o.IsProcessed == false);
            return record.Id;
        }

        public Guid InsertData(byte[] blob)
        {
            var transaction = dbContext.Database.BeginTransaction();
            try
            {
                var newBlob = new Blob { Data = blob };
                dbContext.Blobs.Add(newBlob);
                dbContext.SaveChanges(); // Zapisujemy, aby Entity Framework nadał ID.

                dbContext.Outboxes.Add(new Outbox
                {
                    Id = newBlob.Id,
                    Payload = newBlob.Data, // Na potrzeby testu zostawiamy Payload, inaczej raczej podbierałbym z Blobów
                    IsProcessed = false
                });

                dbContext.SaveChanges();
                transaction.Commit();

                return newBlob.Id;
            }
            catch (Exception)
            {
                transaction.Rollback();
                throw; // Rzucamy wyjątek, aby BasicNack został wywołany w konsumencie
            }
        }
    }
}
