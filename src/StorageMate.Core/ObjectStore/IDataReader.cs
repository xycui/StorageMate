namespace StorageMate.Core.ObjectStore
{
    using System.Threading.Tasks;

    public interface IDataReader<in TKey, TData>
    {
        TData Read(TKey key);
        Task<TData> ReadAsync(TKey key);
    }

    public interface IDataReader<in TKey>
    {
        TData Read<TData>(TKey key);
        Task<TData> ReadAsync<TData>(TKey key);
    }
}
