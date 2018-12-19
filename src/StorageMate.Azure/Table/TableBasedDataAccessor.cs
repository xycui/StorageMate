namespace StorageMate.Azure.Table
{
    using Core.ObjectStore;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;
    using Schema;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    public class TableBasedDataAccessor<TData> : IDataAccessor<string, TData> where TData : new()
    {
        private readonly CloudTable _cloudTable;
        private readonly string _tableName = "TableMateCollection";

        public TableBasedDataAccessor(string storageConnStr) : this(storageConnStr, string.Empty)
        {
        }

        public TableBasedDataAccessor(string storageConnStr, string tableName)
        {
            var account = CloudStorageAccount.Parse(storageConnStr);
            _tableName = !string.IsNullOrEmpty(tableName) ? tableName : _tableName;
            _cloudTable = account.CreateCloudTableClient().GetTableReference(_tableName);
            Task.Run(_cloudTable.CreateIfNotExistsAsync).Wait();
        }

        public TableBasedDataAccessor(CloudStorageAccount storageAccount) : this(storageAccount, string.Empty)
        {
        }

        public TableBasedDataAccessor(CloudStorageAccount storageAccount, string tableName)
        {
            storageAccount = storageAccount ?? throw new ArgumentNullException(nameof(storageAccount));
            _tableName = !string.IsNullOrEmpty(tableName) ? tableName : _tableName;
            _cloudTable = storageAccount.CreateCloudTableClient().GetTableReference(_tableName);
            Task.Run(_cloudTable.CreateIfNotExistsAsync).Wait();
        }

        public TData Read(string key)
        {
            if (string.IsNullOrEmpty(key))
            {
                throw new ArgumentNullException(nameof(key));
            }

            try
            {
                var dataObj = new TData();
                var props = dataObj.GetType().GetProperties();
                var fields = dataObj.GetType().GetFields();
                var propNameSet = new HashSet<string>(props.Select(x => x.Name));
                var fieldNameSet = new HashSet<string>(fields.Select(x => x.Name));
                var propEntityList = _cloudTable.GetKvpAll(key, propNameSet);
                var fieldEntityList = _cloudTable.GetKvpAll(key, fieldNameSet);

                var hasData = false;
                foreach (var entity in propEntityList)
                {
                    var propInfo = dataObj.GetType().GetProperty(entity.RowKey);
                    if (propInfo == null || propInfo.GetSetMethod() == null)
                    {
                        continue;
                    }

                    var propType = propInfo.PropertyType;
                    propInfo.SetValue(dataObj,
                        propType == typeof(string) ? entity.Data : JsonConvert.DeserializeObject(entity.Data, propType),
                        null);
                    hasData = true;
                }

                foreach (var entity in fieldEntityList)
                {

                    var fieldInfo = dataObj.GetType().GetField(entity.RowKey);
                    var fieldType = fieldInfo.FieldType;
                    fieldInfo.SetValue(dataObj,
                        fieldType == typeof(string)
                            ? entity.Data
                            : JsonConvert.DeserializeObject(entity.Data, fieldType));
                    hasData = true;
                }

                return hasData ? dataObj : default(TData);
            }
            catch
            {
                return default(TData);
            }
        }

        public async Task<TData> ReadAsync(string key)
        {
            if (string.IsNullOrEmpty(key))
            {
                throw new ArgumentNullException(nameof(key));
            }

            try
            {
                var dataObj = new TData();
                var props = dataObj.GetType().GetProperties();
                var fields = dataObj.GetType().GetFields();
                var propNameSet = new HashSet<string>(props.Select(x => x.Name));
                var fieldNameSet = new HashSet<string>(fields.Select(x => x.Name));
                var propEntityList = await _cloudTable.GetKvpAllAsync(key, propNameSet);
                var fieldEntityList = await _cloudTable.GetKvpAllAsync(key, fieldNameSet);

                var hasData = false;
                foreach (var entity in propEntityList)
                {
                    var propInfo = dataObj.GetType().GetProperty(entity.RowKey);
                    if (propInfo == null || propInfo.GetSetMethod() == null)
                    {
                        continue;
                    }

                    var propType = propInfo.PropertyType;
                    propInfo.SetValue(dataObj,
                        propType == typeof(string) ? entity.Data : JsonConvert.DeserializeObject(entity.Data, propType),
                        null);
                    hasData = true;
                }

                foreach (var entity in fieldEntityList)
                {
                    var fieldInfo = dataObj.GetType().GetField(entity.RowKey);
                    var fieldType = fieldInfo.FieldType;
                    fieldInfo.SetValue(dataObj,
                        fieldType == typeof(string)
                            ? entity.Data
                            : JsonConvert.DeserializeObject(entity.Data, fieldType));
                    hasData = true;
                }

                return hasData ? dataObj : default(TData);
            }
            catch
            {
                return default(TData);
            }
        }

        public TData Write(string key, TData data)
        {
            return Task.Run(() => WriteAsync(key, data)).Result;
        }

        public async Task<TData> WriteAsync(string key, TData data)
        {
            var props = data.GetType().GetProperties();
            var fields = data.GetType().GetFields();
            var tableStorageEntityList =
                props.Where(x => x.GetGetMethod() != null).Select(
                        x =>
                            new KvpTableEntity
                            {
                                PartitionKey = key,
                                RowKey = x.Name,
                                Data =
                                    x.PropertyType == typeof(string)
                                        ? (string)x.GetValue(data)
                                        : JsonConvert.SerializeObject(x.GetValue(data))
                            })
                    .Concat(
                        fields.Select(
                            y =>
                                new KvpTableEntity()
                                {
                                    PartitionKey = key,
                                    RowKey = y.Name,
                                    Data =
                                        y.FieldType == typeof(string)
                                            ? (string)y.GetValue(data)
                                            : JsonConvert.SerializeObject(y.GetValue(data))
                                }));

            await _cloudTable.InsertOrReplaceBatchAsync(tableStorageEntityList);

            return data;
        }
    }
}
