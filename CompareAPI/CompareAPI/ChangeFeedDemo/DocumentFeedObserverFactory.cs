using Microsoft.Azure.Documents.ChangeFeedProcessor;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using System.Linq.Expressions;
using Microsoft.Azure.Documents.Client;

namespace CompareAPI.ChangeFeedDemo
{
    /// <summary>
    /// Factory for DocumentFeedObjserver (Used in the Change Feed - Sample)
    /// </summary>
    public class DocumentFeedObserverFactory : IChangeFeedObserverFactory
    {
        public DocumentFeedObserverFactory()
        {
        }

        public IChangeFeedObserver CreateObserver()
        {
            return new DocumentFeedObserver();
        }
    }

}
