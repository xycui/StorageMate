namespace StorageMate.Core.ObjectStore
{
    using System.Threading.Tasks;

    public interface IDataWriter<in TKey, TData>
    {
        TData Write(TKey key, TData data);
        Task<TData> WriteAsync(TKey key, TData data);
    }

    public interface IDataWriter<in TKey>
    {
        TData Write<TData>(TKey key, TData data);
        Task<TData> WriteAsync<TData>(TKey key, TData data);
    }
}
