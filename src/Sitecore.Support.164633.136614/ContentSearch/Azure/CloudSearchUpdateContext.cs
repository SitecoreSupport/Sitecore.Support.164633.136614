namespace Sitecore.Support.ContentSearch.Azure
{
    using Sitecore.Abstractions;
    using Sitecore.ContentSearch;
    using Sitecore.ContentSearch.Azure;
    using Sitecore.ContentSearch.Azure.Config;
    using Sitecore.ContentSearch.Azure.Http;
    using Sitecore.ContentSearch.Azure.Utils;
    using Sitecore.ContentSearch.Diagnostics;
    using Sitecore.ContentSearch.Linq.Common;
    using Sitecore.ContentSearch.Sharding;
    using Sitecore.ContentSearch.Utilities;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Sitecore.Diagnostics;
    using Sitecore.Support.ContentSearch.Azure.Utils;

    public class CloudSearchUpdateContext : IProviderUpdateContext, IProviderOperationContext, IDisposable, ITrackingIndexingContext
    {
        private readonly ISearchResultsDeserializer deserializer;
        private readonly ConcurrentBag<CloudSearchDocument> documents;
        private readonly IEvent events;
        private readonly AbstractSearchIndex index;
        private object @lock = new object();
        private ContextOperationStatistics statistics = new ContextOperationStatistics();

        internal CloudSearchUpdateContext(AbstractSearchIndex index, ISearchResultsDeserializer deserializer, ICommitPolicyExecutor commitPolicyExecutor)
        {
            this.index = index;
            this.deserializer = deserializer;
            this.documents = new ConcurrentBag<CloudSearchDocument>();
            this.ParallelOptions = new System.Threading.Tasks.ParallelOptions();
            this.IsParallel = ContentSearchConfigurationSettings.IsParallelIndexingEnabled;
            int parallelIndexingCoreLimit = ContentSearchConfigurationSettings.ParallelIndexingCoreLimit;
            this.events = this.index.Locator.GetInstance<IEvent>();
            if (parallelIndexingCoreLimit > 0)
            {
                this.ParallelOptions.MaxDegreeOfParallelism = parallelIndexingCoreLimit;
            }
            this.CommitPolicyExecutor = commitPolicyExecutor ?? new NullCommitPolicyExecutor();
            this.Processed = new ConcurrentDictionary<IIndexableUniqueId, object>();
        }

        public void AddDocument(object itemToAdd, IExecutionContext executionContext)
        {
            if (itemToAdd == null)
            {
                throw new ArgumentNullException("itemToAdd");
            }
            IExecutionContext[] executionContexts = new IExecutionContext[] { executionContext };
            this.AddDocument(itemToAdd, executionContexts);
        }

        public void AddDocument(object itemToAdd, params IExecutionContext[] executionContexts)
        {
            if (itemToAdd == null)
            {
                throw new ArgumentNullException("itemToAdd");
            }
            IDictionary<string, object> fields = itemToAdd as IDictionary<string, object>;
            if (fields != null)
            {
                object @lock = this.@lock;
                lock (@lock)
                {
                    this.documents.Add(new CloudSearchDocument(fields, SearchAction.Upload));
                    this.CommitPolicyExecutor.IndexModified(this, fields, IndexOperation.Add);
                    this.statistics.IncrementAddCounter();
                }
            }
        }

        public void Commit()
        {
            CrawlingLog.Log.Debug($"[Index={this.index.Name}] Committing: {this.statistics}", null);
            object[] parameters = new object[] { this.index.Name };
            this.events.RaiseEvent("indexing:committing", parameters);
            CloudSearchProviderIndex index = this.Index as CloudSearchProviderIndex;
            if ((index != null) && (this.documents.Count > 0))
            {
                ICloudBatchBuilder builder = index.Locator.GetInstance<IFactoryWrapper>().CreateObject<ICloudBatchBuilder>("contentSearch/cloudBatchBuilder", true);
                try
                {
                    while (!this.documents.IsEmpty)
                    {
                        CloudSearchDocument document;
                        if (this.documents.TryTake(out document))
                        {
                            builder.AddDocument(document);
                        }
                        if (builder.IsFull || this.documents.IsEmpty)
                        {
                            ICloudBatch batch = builder.Release();
                            index.SearchService.PostDocuments(batch);
                            builder.Clear();
                        }
                    }
                }
                catch (Exception exception)
                {
                    foreach (CloudSearchDocument document2 in builder)
                    {
                        this.documents.Add(document2);
                    }
                    CrawlingLog.Log.Error($"[Index={this.index.Name}] Commit failed", exception);
                    throw;
                }
                CrawlingLog.Log.Debug($"[Index={this.index.Name}] Committed", null);
                object[] objArray2 = new object[] { this.index.Name };
                this.events.RaiseEvent("indexing:committed", objArray2);
                this.statistics = new ContextOperationStatistics();
                this.CommitPolicyExecutor.Committed();
            }
        }

        public void Delete(IIndexableId id)
        {
            if (id == null)
            {
                throw new ArgumentNullException("id");
            }
            string indexFieldName = this.Index.FieldNameTranslator.GetIndexFieldName("_id");
            object obj2 = this.Index.Configuration.IndexFieldStorageValueFormatter.FormatValueForIndexStorage(id.Value, indexFieldName);
            string expression = $"&$filter=({indexFieldName} eq '{obj2}')&$select={CloudSearchConfig.VirtualFields.CloudUniqueId}";
            string textResults = (this.Index as Sitecore.ContentSearch.Azure.CloudSearchProviderIndex).SearchService.Search(expression);

            #region Fix Sitecore.Support.164633
            //SearchResults results = this.deserializer.Deserialize(textResults);
            //this.Delete(results.Documents.First<IndexedDocument>().AzureUniqueId);

            SearchResults results = null;

            if (!string.IsNullOrEmpty(textResults))
            {
                try
                {
                    results = this.deserializer.Deserialize(textResults);
                }
                catch (Exception ex)
                {
                    CrawlingLog.Log.Warn($"Item '{id.ToString()}' cannot be found in the '{this.index.Name}' so the document is not deleted from index.", ex);

                    return;
                }

                if (results.Documents.Count > 0)
                {
                    try
                    {
                        this.Delete(results.Documents.First<IndexedDocument>().AzureUniqueId);
                    }
                    catch (Exception ex)
                    {
                        CrawlingLog.Log.Error($"(ItemId:'{id.ToString()}' cannot be deleted from the '{this.index.Name}' index.", ex);

                        return;
                    }

                    return;
                }
            }

            CrawlingLog.Log.Warn($"Item '{id.ToString()}' cannot be found in the '{this.index.Name}' so the document is not deleted from index.", null);
            #endregion
        }

        public void Delete(IIndexableUniqueId id)
        {
            if (id == null)
            {
                throw new ArgumentNullException("id");
            }

            this.Delete(PublicCloudIndexParser.HashUniqueId(id.Value.ToString()));
        }

        private void Delete(string cloudUniqueId)
        {
            ConcurrentDictionary<string, object> concurrentDictionary = new ConcurrentDictionary<string, object>();
            concurrentDictionary.TryAdd(CloudSearchConfig.VirtualFields.CloudUniqueId, cloudUniqueId);
            this.documents.Add(new CloudSearchDocument(concurrentDictionary, SearchAction.Delete));
            object obj = this.@lock;
            lock (obj)
            {
                this.CommitPolicyExecutor.IndexModified(this, cloudUniqueId, IndexOperation.Delete);
                this.statistics.IncrementDeleteUniqueCounter();
            }
        }

        public void Dispose()
        {
        }

        public void Optimize()
        {
        }

        public void UpdateDocument(object itemToUpdate, object criteriaForUpdate, IExecutionContext executionContext)
        {
            IExecutionContext[] executionContexts = new IExecutionContext[] { executionContext };
            this.UpdateDocument(itemToUpdate, criteriaForUpdate, executionContexts);
        }

        public void UpdateDocument(object itemToUpdate, object criteriaForUpdate, params IExecutionContext[] executionContexts)
        {
            if (itemToUpdate == null)
            {
                throw new ArgumentNullException("itemToUpdate");
            }
            IDictionary<string, object> fields = itemToUpdate as IDictionary<string, object>;
            if (fields != null)
            {
                object @lock = this.@lock;
                lock (@lock)
                {
                    this.documents.Add(new CloudSearchDocument(fields, SearchAction.Upload));
                    this.CommitPolicyExecutor.IndexModified(this, fields, IndexOperation.Add);
                    this.statistics.IncrementUpdateCounter();
                }
            }
        }

        public ICommitPolicyExecutor CommitPolicyExecutor { get; private set; }

        public ISearchIndex Index =>
            this.index;

        public bool IsParallel { get; private set; }

        public System.Threading.Tasks.ParallelOptions ParallelOptions { get; private set; }

        public ConcurrentDictionary<IIndexableUniqueId, object> Processed { get; set; }

        public IEnumerable<Shard> ShardsWithPendingChanges { get; private set; }
    }
}