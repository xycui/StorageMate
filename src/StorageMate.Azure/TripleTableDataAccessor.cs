﻿namespace StorageMate.Azure
{
    using System.Threading.Tasks;
    using Core.ObjectStore;
    public class TripleTableDataAccessor<TData> : IDataAccessor<string, TData> where TData : new()
    {
        public TData Read(string key)
        {
            throw new System.NotImplementedException();
        }

        public Task<TData> ReadAsync(string key)
        {
            throw new System.NotImplementedException();
        }

        public TData Write(string key, TData data)
        {
            throw new System.NotImplementedException();
        }

        public Task<TData> WriteAsync(string key, TData data)
        {
            throw new System.NotImplementedException();
        }
    }
}
