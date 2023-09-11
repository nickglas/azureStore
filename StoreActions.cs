using Domain;
using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStoreGeneric;

public class StoreActions
{

    private IConfiguration _configuration;
    private CloudTableClient _cloudTableClient;
    private string _connectionString = "";

    public StoreActions()
    {
        _configuration = new ConfigurationBuilder()
            .AddJsonFile("Appconfig.json")
            .Build();
    }

    public async Task Run()
    {
        try
        {
            //Checking config
            IsConfigValid();

            //Building connection string from json values
            BuildConnectionString();

            //Creating the client to interact with azure
            CreateTableClient();

            //loading the cloud table object
            CloudTable table = LoadCloudTableReference("persons");

            //Creating new table if needed
            await CreateCloudTable(table);

            //create new record
            await InsertRecord(table);
            
            //get all records
            List<Person> persons = await LoadRecords(table);
            foreach (var p in persons)   
            {
                Console.WriteLine($"(STATIC) Found person {p.FirstName} {p.LastName} with id: {p.Id}");
            }
            
            //get single record
            Person foundPerson = await LoadRecord(table, "some_random_id", "NickGlas");
            Console.WriteLine($"(STATIC) Found person {foundPerson.FirstName} {foundPerson.LastName} with filter search");

            //update record
            Person updated = await UpdateRecord(table);
            Console.WriteLine($"(STATIC) Person updated with new firstname {updated.FirstName}");
            
            //Create person generic
            Person newPerson = new Person("some_id_2", "henk", "karels");
            await InsertRecord<Person>("persons", newPerson);
            
            //get all records
            List<Person> entities = await LoadRecords<Person>(table);
            foreach (var p in entities)   
            {
                Console.WriteLine($"(GENERIC) Found person {p.FirstName} {p.LastName} with id: {p.Id}");
            }
            
            //get single method generic
            Person genericP = await LoadRecord<Person>(table, "some_id_2", "henkkarels");
            Console.WriteLine($"(GENERIC) Found person {genericP.FirstName} {genericP.LastName} with filter search");

            //update record
            Person genNew = new Person("some_id_2", "Willen", "Annink");
            Person genericUpdated = await UpdateRecord<Person>(table, genNew, "some_id_2", "henkkarels");
            Console.WriteLine($"(STATIC) Person updated with new firstname {genericUpdated.FirstName}");
            
        }
        catch (StorageException e)
        {
            Console.WriteLine("Error loading/creating table");
            Console.WriteLine(e.Message);
            Console.ReadLine();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
            Console.ReadLine();
        }
    }

    private void IsConfigValid()
    {
        if (string.IsNullOrEmpty(_configuration["DefaultEndpointsProtocol"]) || 
            string.IsNullOrEmpty(_configuration["AccountName"]) || 
            string.IsNullOrEmpty(_configuration["AccountKey"]) || 
            string.IsNullOrEmpty(_configuration["BlobEndpoint"]) || 
            string.IsNullOrEmpty(_configuration["TableEndpoint"]) || 
            string.IsNullOrEmpty(_configuration["QueueEndpoint"]))
        {
            throw new Exception("Invalid appconfig");
        }
    }
    
    private void BuildConnectionString()
    {
        foreach (var setting in _configuration.GetChildren())
        {
            _connectionString += $"{setting.Key}={setting.Value};";
        }
    }

    private void CreateTableClient()
    {
        CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_connectionString);
        _cloudTableClient = storageAccount.CreateCloudTableClient();
    }

    private CloudTable LoadCloudTableReference(string tableName)
    {
        return _cloudTableClient.GetTableReference(tableName);
    }
    
    private async Task CreateCloudTable(CloudTable table)
    {
        await table.CreateIfNotExistsAsync();
    }

    private async Task<Person> InsertRecord(CloudTable table)
    {
        Person p = new Person(Guid.NewGuid().ToString(), "Nick", "Glas");
        p.Id = "some_random_id";
        p.PartitionKey = "some_random_id";
        
        TableOperation tableOperation = TableOperation.Insert(p);
        TableResult result = await table.ExecuteAsync(tableOperation);
        return result.Result as Person;
    }

    private async Task<T> InsertRecord<T>(CloudTable table, T entity) where T: class, ITableEntity, new()
    {
        TableOperation tableOperation = TableOperation.Insert(entity);
        TableResult result = await table.ExecuteAsync(tableOperation);
        return result.Result as T;
    }
    
    private async Task<T> InsertRecord<T>(string tableName, T entity) where T: class, ITableEntity, new()
    {
        CloudTable table = LoadCloudTableReference(tableName);
        TableOperation tableOperation = TableOperation.Insert(entity);
        TableResult result = await table.ExecuteAsync(tableOperation);
        return result.Result as T;
    }

    private async Task<List<Person>> LoadRecords(CloudTable table)
    {
        List<Person> persons = new List<Person>();
        var query = new TableQuery<Person>();
        TableContinuationToken continuationToken = null;
        do
        {
            var page = await table.ExecuteQuerySegmentedAsync(query, continuationToken);
            continuationToken = page.ContinuationToken;
            persons.AddRange(page.Results);
        }
        while (continuationToken != null);

        return persons;
    }
    
    private async Task<List<T>> LoadRecords<T>(CloudTable table) where T: class, ITableEntity, new()
    {
        List<T> entities = new List<T>();
        var query = new TableQuery<T>();
        TableContinuationToken continuationToken = null;
        do
        {
            var page = await table.ExecuteQuerySegmentedAsync(query, continuationToken);
            continuationToken = page.ContinuationToken;
            entities.AddRange(page.Results);
        }
        while (continuationToken != null);

        return entities;
    }

    private async Task<Person> LoadRecord(CloudTable table, string partitionKey, string rowKey)
    {
        TableOperation tableOperation = TableOperation.Retrieve<Person>(partitionKey, rowKey);
        TableResult tableResult = await table.ExecuteAsync(tableOperation);
        return tableResult.Result as Person;
    }
    private async Task<T> LoadRecord<T>(CloudTable table, string partitionKey, string rowKey) where T : class, ITableEntity, new()
    {
        TableOperation tableOperation = TableOperation.Retrieve<T>(partitionKey, rowKey);
        TableResult tableResult = await table.ExecuteAsync(tableOperation);
        return tableResult.Result as T;
    }

    private async Task<Person> UpdateRecord(CloudTable table)
    {
        Person p = await LoadRecord(table, "some_random_id","NickGlas");
        if (p is not null)
        {
            p.FirstName = "Simon";
            TableOperation tableOperation = TableOperation.Replace(p);
            var result = await table.ExecuteAsync(tableOperation);
        }

        return p;
    }

    private async Task<T> UpdateRecord<T>(CloudTable table, T newData, string partitionKey, string rowKey) where T: class, ITableEntity, new()
    {
        T entity = await LoadRecord<T>(table, partitionKey, rowKey);
        if (entity is not null)
        {
            Type entityType = typeof(T);
            
            foreach (var propertyInfo in entityType.GetProperties())
            {
                var newDataProperty = newData.GetType().GetProperty(propertyInfo.Name);
                if (newDataProperty != null && newDataProperty.CanWrite && propertyInfo.CanWrite)
                {
                    var newValue = newDataProperty.GetValue(newData, null);
                    propertyInfo.SetValue(entity, newValue, null);
                }
            }

            entity.ETag = "*";
            TableOperation tableOperation = TableOperation.Replace(entity);
            var result = await table.ExecuteAsync(tableOperation);
        }

        return entity;
    }
    
}