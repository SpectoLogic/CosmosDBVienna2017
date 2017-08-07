using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.ChangeFeedProcessor;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CompareAPI
{
    /// <summary>
    /// DocumentFeedObserver which gets instantiated by DocumentFeedObserverFactory which gets instantiated by ChangeFeedEventHost
    /// (Used in the Change Feed - Sample)
    /// </summary>
    public class DocumentFeedObserver : IChangeFeedObserver
    {
        public DocumentFeedObserver()
        {
        }

        /// <summary>
        /// This method should be called before the processing of changes starts,...
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public Task OpenAsync(ChangeFeedObserverContext context)
        {
            return Task.CompletedTask; // Framework 4.6 above
        }

        /// <summary>
        /// Called after OpenAsnc
        /// </summary>
        /// <param name="context"></param>
        /// <param name="docs"></param>
        /// <returns></returns>
        public Task ProcessChangesAsync(ChangeFeedObserverContext context, IReadOnlyList<Document> docs)
        {
            Console.WriteLine($"    Processing partition key range: {context.PartitionKeyRangeId} - {docs.Count()} changed documents...");
            foreach (Person changedPerson in docs)
            {
                Console.WriteLine($"        Changed Person: {changedPerson.name}!");
            }
            return Task.CompletedTask;
        }

        public Task CloseAsync(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            if (reason == ChangeFeedObserverCloseReason.LeaseLost)
            {
                // The lease is lost by ChangeFeedEventHost but it still exists and ChangeFeedEventHost
                // keeps calling ProcessAsync which is IMHO a bug. 
                // There is an open issue on git hub: https://github.com/Azure/azure-documentdb-dotnet/issues/237
                // Just be aware of this, if you create/dispose objects. Do not dispose objects immediatly in case of "LeaseLost"
                // as workaround. At some later point in time the ChangeFeedEventHost disposes the Factory and creates a new one
            }
            return Task.CompletedTask; // Framework 4.6 above
        }
    }
}
