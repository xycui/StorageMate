namespace StorageMate.Core.ObjectStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Threading.Tasks;

    public interface IDataInsighter<TData>
    {
        IEnumerable<TProperty> ListAll<TProperty>(Expression<Func<TData, TProperty>> expression);
        Task<IList<TProperty>> ListAllAsync<TProperty>(Expression<Func<TData, TProperty>> expression);
        IEnumerable<TProperty> ListWithCondition<TProperty, TCondition>(Expression<Func<TData, TCondition>> condition, TCondition value,
            Expression<Func<TData, TProperty>> expression);
        Task<IList<TProperty>> ListWithConditionAsync<TProperty, TCondition>(Expression<Func<TData, TCondition>> condition, TCondition value,
            Expression<Func<TData, TProperty>> expression);
        Task AddForStatsAsync(TData data);
        Task AddForStatsAsync(TData data, params Expression<Func<TData, object>>[] targetExpressions);
        Task AddStatsWithCondition(TData data, Expression<Func<TData, object>> conditionExpression);
    }

    public enum Condition
    {
        Eq,
        Gt,
        Ge,
        Lt,
        Le
    }
}
