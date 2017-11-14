namespace Sitecore.Support.ContentSearch.Azure
{
  using System;
  using System.Reflection;
  using Sitecore.ContentSearch;
  using Sitecore.ContentSearch.Azure.Http;
  using Sitecore.ContentSearch.Azure.Schema;
  using Sitecore.ContentSearch.Maintenance;
  using Sitecore.ContentSearch.Security;

    public class CloudSearchProviderIndex : Sitecore.ContentSearch.Azure.CloudSearchProviderIndex
    {
        private ISearchResultsDeserializer deserializer;
        private Action dEnsureInitialized;

        public CloudSearchProviderIndex(string name, string connectionStringName, string totalParallelServices,
            IIndexPropertyStore propertyStore) : base(name, connectionStringName, totalParallelServices, propertyStore)
        {
            this.InitializeDelegates();
        }

        public CloudSearchProviderIndex(string name, string connectionStringName, string totalParallelServices,
            IIndexPropertyStore propertyStore, string @group)
            : base(name, connectionStringName, totalParallelServices, propertyStore, @group)
        {
            this.InitializeDelegates();
        }

        public override void Initialize()
        {
            base.Initialize();
            var d = typeof(Sitecore.ContentSearch.Azure.CloudSearchProviderIndex).GetField("deserializer", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(this);
            this.deserializer = (ISearchResultsDeserializer)d;
        }  

        public override IProviderUpdateContext CreateUpdateContext()
        {
            this.dEnsureInitialized();
            ICommitPolicyExecutor commitPolicyExecutor = (ICommitPolicyExecutor)this.CommitPolicyExecutor.Clone();
            commitPolicyExecutor.Initialize(this);
            return new CloudSearchUpdateContext(this, this.deserializer, commitPolicyExecutor);
        }


        #region Workaround for issue 136614

        public new ICloudSearchIndexSchemaBuilder SchemaBuilder
        {
            get { return (this as Sitecore.ContentSearch.Azure.CloudSearchProviderIndex).SchemaBuilder; }
            set
            {
                var pi = typeof(Sitecore.ContentSearch.Azure.CloudSearchProviderIndex)
                    .GetProperty("SchemaBuilder", BindingFlags.Instance | BindingFlags.Public);
                pi.SetValue(this, value);
            }
        }


        public new ISearchService SearchService
        {
            get { return (this as Sitecore.ContentSearch.Azure.CloudSearchProviderIndex).SearchService; }
            set
            {
                var pi = typeof(Sitecore.ContentSearch.Azure.CloudSearchProviderIndex)
                    .GetProperty("SearchService", BindingFlags.Instance | BindingFlags.Public);
                pi.SetValue(this, value);
            }
        }

        #endregion

        protected void InitializeDelegates()
        {
            var t = typeof(Sitecore.ContentSearch.Azure.CloudSearchProviderIndex);
            var m = t.GetMethod("EnsureInitialized", BindingFlags.Instance | BindingFlags.NonPublic);
            this.dEnsureInitialized = m.CreateDelegate(typeof(Action), this) as Action;
        }
    }
}