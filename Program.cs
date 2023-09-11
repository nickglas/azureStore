namespace AzureTableStoreGeneric
{
    internal class Program
    {
        static void Main(string[] args)
        {
            StoreActions storeActions = new StoreActions();
            storeActions.Run().Wait();
            Console.WriteLine("Done");
        }
    }
}