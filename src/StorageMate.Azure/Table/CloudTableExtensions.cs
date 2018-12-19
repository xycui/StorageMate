namespace StorageMate.Azure.Table
{
    using Microsoft.WindowsAzure.Storage.Table;
    using Schema;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    public static class CloudTableExtensions
    {
        public static IEnumerable<KvpTableEntity> GetKvpAll(this CloudTable table, string partitionKey)
        {
            var query =
                new TableQuery<KvpTableEntity>().Where(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));
            TableContinuationToken token = null;
            do
            {
                var segementResult = Task.Run(async () => await table.ExecuteQuerySegmentedAsync(query, new TableContinuationToken())).Result;
                token = segementResult.ContinuationToken;
                foreach (var result in segementResult.Results)
                {
                    yield return result;
                }
            } while (token != null);
        }

        public static async Task<IList<KvpTableEntity>> GetKvpAllAsync(this CloudTable table, string partitionKey)
        {
            var ret = new List<KvpTableEntity>();
            var query =
                new TableQuery<KvpTableEntity>().Where(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));
            TableContinuationToken token = null;
            do
            {
                var segementResult = await table.ExecuteQuerySegmentedAsync(query, new TableContinuationToken());
                token = segementResult.ContinuationToken;
                ret.AddRange(segementResult.Results);
            } while (token != null);

            return ret;
        }

        public static IEnumerable<KvpTableEntity> GetKvpAll(this CloudTable table, string partitionKey,
            ISet<string> targetRowKeys)
        {
            if (targetRowKeys == null || !targetRowKeys.Any())
            {
                yield break;
            }

            var partitionKeyFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
            var rowKeyFilters = targetRowKeys.Select(m => TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, m)).ToList();
            var filterString = $"({partitionKeyFilter}) and ({string.Join(" or ", rowKeyFilters)})";
            var tableQuery = new TableQuery<KvpTableEntity>().Where(filterString);

            TableContinuationToken token = null;
            do
            {
                var segmentResult = Task.Run(async () =>
                        await table.ExecuteQuerySegmentedAsync(tableQuery, new TableContinuationToken())).Result;
                token = segmentResult.ContinuationToken;

                foreach (var result in segmentResult.Results)
                {
                    yield return result;
                }
            } while (token != null);
        }

        public static async Task<IList<KvpTableEntity>> GetKvpAllAsync(this CloudTable table, string partitionKey,
            ISet<string> targetRowKeys)
        {
            if (targetRowKeys == null || !targetRowKeys.Any())
            {
                return null;
            }

            var ret = new List<KvpTableEntity>();
            var partitionKeyFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
            var rowKeyFilters = targetRowKeys.Select(m => TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, m)).ToList();
            var filterString = $"({partitionKeyFilter}) and ({string.Join(" or ", rowKeyFilters)})";
            var tableQuery = new TableQuery<KvpTableEntity>().Where(filterString);

            TableContinuationToken token;
            do
            {
                var segementResult = await table.ExecuteQuerySegmentedAsync(tableQuery, new TableContinuationToken());
                token = segementResult.ContinuationToken;
                ret.AddRange(segementResult.Results);
            } while (token != null);

            return ret;
        }

        public static async Task InsertOrReplaceBatchAsync(this CloudTable table, IEnumerable<KvpTableEntity> entityBatch)
        {
            var batchOperation = new TableBatchOperation();
            foreach (var entity in entityBatch)
            {
                batchOperation.Add(TableOperation.InsertOrReplace(entity));
                if (batchOperation.Count != 100)
                {
                    continue;
                }

                await table.ExecuteBatchAsync(batchOperation).ConfigureAwait(false);
                batchOperation.Clear();
            }

            if (batchOperation.Count > 0)
            {
                await table.ExecuteBatchAsync(batchOperation).ConfigureAwait(false);
            }
        }
    }
}
