namespace StorageMate.Azure.Table
{
    using Core.ObjectStore;
    using Core.Stats;
    using Core.Utils;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;
    using Schema;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;

    public class TableDataInsighter<TData> : IDataInsighter<TData>
    {
        private readonly CloudTable _cloudTable;
        private readonly string _tableName = "TableMateStatsCollection";

        public TableDataInsighter(string storageConnStr) : this(storageConnStr, string.Empty)
        {
        }

        public TableDataInsighter(string storageConnStr, string tableName)
        {
            var account = CloudStorageAccount.Parse(storageConnStr);
            _tableName = !string.IsNullOrEmpty(tableName) ? tableName : _tableName;
            _cloudTable = account.CreateCloudTableClient().GetTableReference(_tableName);
            Task.Run(_cloudTable.CreateIfNotExistsAsync).Wait();
        }

        public TableDataInsighter(CloudStorageAccount storageAccount) : this(storageAccount, string.Empty)
        {
        }

        public TableDataInsighter(CloudStorageAccount storageAccount, string tableName)
        {
            storageAccount = storageAccount ?? throw new ArgumentNullException(nameof(storageAccount));
            _tableName = !string.IsNullOrEmpty(tableName) ? tableName : _tableName;
            _cloudTable = storageAccount.CreateCloudTableClient().GetTableReference(_tableName);
            Task.Run(_cloudTable.CreateIfNotExistsAsync).Wait();
        }

        public IEnumerable<TProperty> ListAll<TProperty>(Expression<Func<TData, TProperty>> expression)
        {
            var bodyExp = GetMemberExpBody(expression);

            foreach (var item in _cloudTable.GetKvpAll(GetPartitionKey(bodyExp)))
            {
                var data = default(TProperty);
                try
                {
                    data = JsonConvert.DeserializeObject<TProperty>(item.Data);
                }
                catch
                {
                    //ignore
                }

                if (data != null)
                {
                    yield return data;
                }
            }
        }

        public async Task<IList<TProperty>> ListAllAsync<TProperty>(Expression<Func<TData, TProperty>> expression)
        {
            var bodyExp = GetMemberExpBody(expression);

            var kvpData =
                (await _cloudTable.GetKvpAllAsync(GetPartitionKey(bodyExp))).Select(x =>
                {
                    try
                    {
                        return JsonConvert.DeserializeObject<TProperty>(x.Data);
                    }
                    catch
                    {
                        return default(TProperty);
                    }
                });

            return kvpData.Where(x => x != null).ToList();
        }

        public IEnumerable<TProperty> ListWithCondition<TProperty, TCondition>(Expression<Func<TData, TCondition>> condition, TCondition value,
            Expression<Func<TData, TProperty>> expression)
        {
            var condBodyExp = GetMemberExpBody(condition);
            var targetBodyExp = GetMemberExpBody(expression);

            var partitionKey = GetPartitionKey(condBodyExp, value, targetBodyExp);

            foreach (var item in _cloudTable.GetKvpAll(partitionKey))
            {
                var data = default(TProperty);
                try
                {
                    data = JsonConvert.DeserializeObject<TProperty>(item.Data);
                }
                catch
                {
                    //ignore
                }

                if (data != null)
                {
                    yield return data;
                }
            }
        }

        public async Task<IList<TProperty>> ListWithConditionAsync<TProperty, TCondition>(Expression<Func<TData, TCondition>> condition, TCondition value,
            Expression<Func<TData, TProperty>> expression)
        {
            var condBodyExp = GetMemberExpBody(condition);
            var targetBodyExp = GetMemberExpBody(expression);

            var partitionKey = GetPartitionKey(condBodyExp, value, targetBodyExp);

            var kvpData =
                (await _cloudTable.GetKvpAllAsync(partitionKey)).Select(x =>
                {
                    try
                    {
                        return JsonConvert.DeserializeObject<TProperty>(x.Data);
                    }
                    catch
                    {
                        return default(TProperty);
                    }
                });

            return kvpData.Where(x => x != null).ToList();
        }

        public async Task AddForStatsAsync(TData data)
        {
            ISet<StatsMemberBag> conditionSet = new HashSet<StatsMemberBag>();
            ISet<StatsMemberBag> targetSet = new HashSet<StatsMemberBag>();

            OnBoardPropStats(data, conditionSet, targetSet);
            OnBoardFieldStats(data, conditionSet, targetSet);

            var kvpDataItems = GenerateEntities(conditionSet, targetSet);
            var groups = kvpDataItems.GroupBy(entity => entity.PartitionKey);
            foreach (var group in groups)
            {
                await _cloudTable.InsertOrReplaceBatchAsync(group);
            }
        }

        public Task AddForStatsAsync(TData data, params Expression<Func<TData, object>>[] targetExpressions)
        {
            throw new NotImplementedException();
        }

        public Task AddStatsWithCondition(TData data, Expression<Func<TData, object>> conditionExpression)
        {
            throw new NotImplementedException();
        }


        private static MemberExpression GetMemberExpBody(LambdaExpression expression)
        {
            if (!(expression.Body is MemberExpression body))
            {
                throw new ArgumentException("'expression' should be a member expression");
            }

            return body;
        }


        private static string GetPartitionKey<TCondition>(MemberExpression conditionExp,
            TCondition value, MemberExpression targetExp)
        {
            var content = typeof(TCondition) == typeof(string) ? value.ToString() : JsonConvert.SerializeObject(value);
            content = HashUtil.ComputeMd5Hash(content);

            var partitionKey =
                $"{typeof(TData)}_{conditionExp.Type}_{conditionExp.Member.Name}_{content}_{targetExp.Type}_{targetExp.Member.Name}";

            return partitionKey;
        }

        private static string GetPartitionKey(MemberExpression targetExp)
        {
            var partitionKey =
                $"{typeof(TData)}_{targetExp.Type}_{targetExp.Member.Name}";

            return partitionKey;
        }

        private static void OnBoardPropStats(TData data, ISet<StatsMemberBag> conditionBag, ISet<StatsMemberBag> targetBag)
        {
            foreach (var prop in typeof(TData).GetProperties())
            {
                var targetAttrs = prop.GetCustomAttributes(typeof(StatsTargetAttribute), false);
                var conditionAttrs = prop.GetCustomAttributes(typeof(StatsConditionAttribute), false);
                var getter = prop.GetGetMethod();
                if (getter == null)
                {
                    continue;
                }

                var propType = prop.PropertyType;
                var memberBag = new StatsMemberBag
                {
                    MemberType = propType,
                    MemberName = prop.Name,
                    MemberValue =
                        typeof(string) == propType
                            ? (string)prop.GetValue(data)
                            : JsonConvert.SerializeObject(prop.GetValue(data))
                };
                if (targetAttrs.Any())
                {
                    targetBag.Add(memberBag);
                }

                if (conditionAttrs.Any())
                {
                    conditionBag.Add(memberBag);
                }
            }
        }

        private static void OnBoardFieldStats(TData data, ISet<StatsMemberBag> conditionBag, ISet<StatsMemberBag> targetBag)
        {
            foreach (var field in typeof(TData).GetFields())
            {
                var targetAttrs = field.GetCustomAttributes(typeof(StatsTargetAttribute), false);
                var conditionAttrs = field.GetCustomAttributes(typeof(StatsConditionAttribute), false);

                var propType = field.FieldType;
                var memberBag = new StatsMemberBag
                {
                    MemberType = propType,
                    MemberName = field.Name,
                    MemberValue =
                        typeof(string) == propType
                            ? (string)field.GetValue(data)
                            : JsonConvert.SerializeObject(field.GetValue(data))
                };
                if (targetAttrs.Any())
                {
                    targetBag.Add(memberBag);
                }

                if (conditionAttrs.Any())
                {
                    conditionBag.Add(memberBag);
                }
            }
        }

        private static IEnumerable<KvpTableEntity> GenerateEntities(ISet<StatsMemberBag> conditionBag,
            ISet<StatsMemberBag> targetBag)
        {
            foreach (var bag in targetBag)
            {
                var partionKey = $"{typeof(TData)}_{bag.MemberType}_{bag.MemberName}";
                var rowKey = HashUtil.ComputeMd5Hash(bag.MemberValue);
                yield return new KvpTableEntity(partionKey, rowKey, bag.MemberValue);
            }

            foreach (var bag in targetBag)
            {
                foreach (var conBag in conditionBag)
                {
                    if (conBag.Equals(bag))
                    {
                        continue;
                    }

                    var condContent = HashUtil.ComputeMd5Hash(conBag.MemberValue);
                    var partionKey = $"{typeof(TData)}_{conBag.MemberType}_{conBag.MemberName}_{condContent}_{bag.MemberType}_{bag.MemberName}";
                    var rowKey = HashUtil.ComputeMd5Hash(bag.MemberValue);

                    yield return new KvpTableEntity(partionKey, rowKey, bag.MemberValue);
                }
            }
        }
    }
}
