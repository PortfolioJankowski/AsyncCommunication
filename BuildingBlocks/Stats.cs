public static class Stats
{
    public const string exchange_name = "amq.direct";
    public const string blob_requests_queue = "blob_queue";
    public const string blob_requests_take_blob_key = "get_blob_key";
    public const string blob_responses_queue = "blob_responses_queue";
    public const string blob_responses_take_blob_key = "take_blob_key";

    public const string dead_letter_exchange_name = "dlx_exchange";
    public const string dead_letter_queue_name = "dlx_queue";
    public const string dead_letter_routing_key = "dlx_routing_key";
    public const string dead_letter_message_ttl = "60000"; // czas życia wiadomości w ms
    public const string dead_end_point_queue_name = "dead_end_point_queue";
}
