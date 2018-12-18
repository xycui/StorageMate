namespace StorageMate.Tests
{
    using Azure.Table;
    using Microsoft.WindowsAzure.Storage;
    using NUnit.Framework;
    using System;


    internal class MockData
    {
        public string Id;
        public string Data;
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
    }
}