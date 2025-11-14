using Microsoft.EntityFrameworkCore;

namespace BuildingBlocks.Db;

public class ApiDbContext : DbContext
{
    public DbSet<Blob> Blobs { get; set; }
    public DbSet<Outbox> Outboxes { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseSqlite("Data Source=database.db");
    }
}
