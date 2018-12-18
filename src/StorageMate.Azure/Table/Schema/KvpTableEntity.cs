namespace StorageMate.Azure.Table.Schema
{
    using Microsoft.WindowsAzure.Storage.Table;
    public class KvpTableEntity : TableEntity
    {
        public KvpTableEntity()
        {
        }

        public KvpTableEntity(string partitionKey, string rowKey, string data)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            Data = data;
        }

        public string Data { get; set; }
    }
}
