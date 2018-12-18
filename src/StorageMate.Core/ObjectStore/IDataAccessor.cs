namespace StorageMate.Core.ObjectStore
{
    public interface IDataAccessor<in TKey> : IDataReader<TKey>, IDataWriter<TKey>
    {
    }

    public interface IDataAccessor<in TKey, TData> : IDataReader<TKey, TData>, IDataWriter<TKey, TData>
    {
    }
}
