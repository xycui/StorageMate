namespace StorageMate.Tests
{
    using Azure.Table;
    using Microsoft.WindowsAzure.Storage;
    using NUnit.Framework;
    using System;
    using System.Linq.Expressions;


    internal class MockData
    {
        public string Id;
        public string Data;
        public MockData Test { get; set; }
    }

    public class TableBaseDataAccessorTests
    {
        private const string RemoteAzureConnStr = "";

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        [Ignore("Azurite is not implementing the batch operation")]
        public void TestReadWriteObject()
        {
            var mockData = new MockData { Id = Guid.NewGuid().ToString("N"), Data = "Test" };
            var accessor = new TableBasedDataAccessor<MockData>(CloudStorageAccount.DevelopmentStorageAccount);
            var data = accessor.Read(mockData.Id);
            Assert.IsNull(data);
            data = accessor.Write(mockData.Id, mockData);
            data = accessor.Read(mockData.Id);
            Assert.IsNotNull(data);
            Assert.True(mockData.Id == data.Id && mockData.Data == data.Data);
            Assert.Pass();
        }

        [Test]
        public void TestReadWriteObjectAzure()
        {
            _ = string.IsNullOrEmpty(RemoteAzureConnStr)
                ? throw new IgnoreException($"{nameof(RemoteAzureConnStr)} is empty, ignore test")
                : RemoteAzureConnStr;

            var mockData = new MockData { Id = Guid.NewGuid().ToString("N"), Data = "Test" };
            var accessor = new TableBasedDataAccessor<MockData>("");
            var data = accessor.Read(mockData.Id);
            Assert.IsNull(data);
            data = accessor.Write(mockData.Id, mockData);
            data = accessor.Read(mockData.Id);
            Assert.IsNotNull(data);
            Assert.True(mockData.Id == data.Id && mockData.Data == data.Data);
            Assert.Pass();
        }

        [Test]
        public void MemberExpressionTest()
        {
            Expression<Func<MockData, object>> exp1 = data => data.Data;
            Expression<Func<MockData, object>> exp2 = data => data.Test;

            Assert.True(exp1.Body is MemberExpression);
            Assert.True(exp2.Body is MemberExpression);

            var body1 = (MemberExpression)exp1.Body;
            var body2 = (MemberExpression)exp2.Body;

            Console.WriteLine($"{exp1.ReturnType}_{exp1.CanReduce}_{exp1.Name}_{exp1}_{exp1.Type}_{exp1.Body}");
            Console.WriteLine($"{exp2.ReturnType}_{exp2.CanReduce}_{exp2.Name}_{exp2}_{exp2.Type}_{exp2.Body}");

            Console.WriteLine($"{body1.Expression.Type}_{body1.Type}_{body1.Member.Name}_{body1.Member.MemberType}_{body1.Member.ReflectedType}");
            Console.WriteLine($"{body2.Expression.Type}_{body2.Type}_{body2.Member.Name}_{body2.Member.MemberType}_{body2.Member.ReflectedType}");
            Assert.Pass();
        }
    }
}