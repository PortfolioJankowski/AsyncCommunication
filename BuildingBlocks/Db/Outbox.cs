namespace BuildingBlocks.Db;

public class Outbox
{
    public Guid Id { get; set; }
    public byte[] Payload { get; set; }
    public bool IsProcessed { get; set; } = false;
}
