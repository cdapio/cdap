/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.metadata;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.metadata.dataset.MdsHistoryKey;
import co.cask.cdap.data2.metadata.dataset.MdsKey;
import co.cask.cdap.data2.metadata.dataset.Metadata;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.MetadataDatasetDefinition;
import co.cask.cdap.data2.metadata.dataset.MetadataEntries;
import co.cask.cdap.data2.metadata.dataset.MetadataEntry;
import co.cask.cdap.data2.metadata.dataset.MetadataV1;
import co.cask.cdap.data2.metadata.dataset.SearchRequest;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.NoOpOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.impersonation.UnsupportedUGIProvider;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for Metadata Migrator Service.
 */
public class MetadataMigratorTest {
  private final ApplicationId app1 = new ApplicationId("ns1", "app1");
  private final DatasetId dataset1 = new DatasetId("ns1", "ds1");
  private final StreamId stream1 = new StreamId("ns1", "s1");
  private final ArtifactId artifact1 = new ArtifactId("ns1", "a1", "1.0.0");

  private static CConfiguration cConf;
  private TransactionManager txManager;
  private TransactionSystemClient transactionSystemClient;
  private DatasetService datasetService;
  private DatasetFramework datasetFramework;
  private Transactional transactional;

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Before
  public void init() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.Metadata.MIGRATOR_BATCH_SIZE, "5");

    Injector injector = getInjector();

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    transactionSystemClient = injector.getInstance(TransactionSystemClient.class);

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    datasetFramework = injector.getInstance(DatasetFramework.class);

    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), transactionSystemClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
  }

  @After
  public void stop() {
    datasetService.stopAndWait();
    txManager.stopAndWait();
  }

  /**
   * Tests data migration from V1 MetadataDataset to V2 MetadataDataset.
   */
  @Test
  public void testMetadataMigration() throws Exception {
    DatasetId v1SystemDatasetId = NamespaceId.SYSTEM.dataset("system.metadata");
    DatasetId v1BusinessDatasetId = NamespaceId.SYSTEM.dataset("business.metadata");
    DatasetId v2SystemDatasetId = NamespaceId.SYSTEM.dataset("v2.system.metadata");
    DatasetId v2BusinessDatasetId = NamespaceId.SYSTEM.dataset("v2.business.metadata");

    // We will keep track of last timestamp so that we can verify if the history rows are written with existing ts.
    long sTs = generateMetadata(v1SystemDatasetId);
    long bTs = generateMetadata(v1BusinessDatasetId);

    MetadataMigrator migrator = new MetadataMigrator(cConf, datasetFramework, transactionSystemClient);
    migrator.start();

    // Wait for migrator to finish before reading v2 tables
    Tasks.waitFor(true, () -> migrator.state().equals(Service.State.TERMINATED), 5, TimeUnit.MINUTES);

    Transactionals.execute(transactional, context -> {
      MetadataDataset v2System = getMetadataDataset(context, v2SystemDatasetId);
      MetadataDataset v2Business = getMetadataDataset(context, v2BusinessDatasetId);

      assertProperties(v2System, v2Business);
      assertHistory(v2System, v2Business, sTs, bTs);
      assertIndex(v2System);
      assertIndex(v2Business);
    });

    if (datasetFramework.hasInstance(v1SystemDatasetId) || datasetFramework.hasInstance(v1BusinessDatasetId)) {
      throw new Exception("V1 metadata table was not deleted by Metadata Migrator.");
    }
  }

  /**
   * Tests batch scanning and deletes on V1 MetadataDataset.
   */
  @Test
  public void testScanAndDelete() throws Exception {
    DatasetId v1SystemDatasetId = NamespaceId.SYSTEM.dataset("system.metadata");
    DatasetId v1BusinessDatasetId = NamespaceId.SYSTEM.dataset("business.metadata");

    generateMetadata(v1SystemDatasetId);
    generateMetadata(v1BusinessDatasetId);

    Transactionals.execute(transactional, context -> {
      MetadataDataset v1System = getMetadataDataset(context, v1SystemDatasetId);
      int total = 0;
      int scanCount;
      do {
        MetadataEntries entries = v1System.scanFromV1Table(2);
        scanCount = entries.getEntries().size();
        v1System.deleteRows(entries.getRows());
        total = total + scanCount;
      } while (scanCount != 0);

      Assert.assertEquals(9, total);
    });
  }

  private void assertProperties(MetadataDataset v2System, MetadataDataset v2Business) {
    Assert.assertEquals("avalue11", v2System.getProperties(app1.toMetadataEntity()).get("akey1"));
    Assert.assertEquals("avalue3", v2System.getProperties(dataset1.toMetadataEntity()).get("akey3"));
    Assert.assertEquals("avalue4", v2System.getProperties(stream1.toMetadataEntity()).get("akey4"));
    Assert.assertEquals("avalue6", v2System.getProperties(artifact1.toMetadataEntity()).get("akey6"));

    Assert.assertEquals("avalue11", v2Business.getProperties(app1.toMetadataEntity()).get("akey1"));
    Assert.assertEquals("avalue3", v2Business.getProperties(dataset1.toMetadataEntity()).get("akey3"));
    Assert.assertEquals("avalue4", v2Business.getProperties(stream1.toMetadataEntity()).get("akey4"));
    Assert.assertEquals("avalue6", v2Business.getProperties(artifact1.toMetadataEntity()).get("akey6"));
  }

  private void assertHistory(MetadataDataset v2System, MetadataDataset v2Business, long sTs, long bTs) {
    verifyhistory(v2System, app1.toMetadataEntity(), sTs);
    verifyhistory(v2System, dataset1.toMetadataEntity(), sTs);
    verifyhistory(v2System, stream1.toMetadataEntity(), sTs);
    verifyhistory(v2System, artifact1.toMetadataEntity(), sTs);

    verifyhistory(v2Business, app1.toMetadataEntity(), bTs);
    verifyhistory(v2Business, dataset1.toMetadataEntity(), bTs);
    verifyhistory(v2Business, stream1.toMetadataEntity(), bTs);
    verifyhistory(v2Business, artifact1.toMetadataEntity(), bTs);
  }

  private void assertIndex(MetadataDataset v2System) throws Exception {
    SearchRequest sr = new SearchRequest(new NamespaceId("ns1"), "avalue1",
                                              ImmutableSet.of(EntityTypeSimpleName.ALL), SortInfo.DEFAULT, 0,
                                              Integer.MAX_VALUE, 1, null, false, EnumSet.of(EntityScope.USER));


    List<MetadataEntry> entries = v2System.search(sr).getResults();

    for (MetadataEntry entry : entries) {
      Assert.assertEquals("avalue1", entry.getValue());
    }
  }

  private void verifyhistory(MetadataDataset v2, MetadataEntity entity, long timestamp) {
    for (Metadata metadata : v2.getSnapshotBeforeTime(ImmutableSet.of(entity), timestamp)) {
      Map<String, String> properties = metadata.getProperties();
      Assert.assertEquals(1, properties.size());
    }
  }

  private long generateMetadata(DatasetId datasetId) throws Exception {
    // Set some properties
    write(datasetId, app1, "akey1", "avalue1");
    write(datasetId, dataset1, "akey3", "avalue3");
    write(datasetId, stream1, "akey4", "avalue4");
    write(datasetId, artifact1, "akey6", "avalue6");
    return write(datasetId, app1, "akey1", "avalue11");
  }

  private long write(DatasetId datasetId, NamespacedEntityId targetId, String key, String value) throws Exception {
    Put valuePut = createValuePut(targetId, key, value);
    long time = System.currentTimeMillis();
    Put historyPut = createHistoryPut(targetId, time);

    Transactionals.execute(transactional, context -> {
      // Create metadata dataset to access underlying indexed table
      getMetadataDataset(context, datasetId);

      getIndexedTable(context, datasetId).put(valuePut);
      getIndexedTable(context, datasetId).put(historyPut);
    }, Exception.class);

    return time;
  }

  private Put createValuePut(NamespacedEntityId targetId, String key, String value) {
    MDSKey mdsValueKey = MdsKey.getMDSValueKey(targetId, key);
    Put put = new Put(mdsValueKey.getKey());

    // add the metadata value
    byte[] valueRowPrefix = {'v'};
    put.add(valueRowPrefix, Bytes.toBytes(value));
    return put;
  }

  private Put createHistoryPut(NamespacedEntityId targetId, long time) {
    MetadataV1 metadataV1 = getMetadataV1(targetId);
    byte[] row = MdsHistoryKey.getMdsKey(targetId, time).getKey();
    String jsonValue = getJsonValue(targetId.getEntityType());

    Put put = new Put(row);
    put.add(Bytes.toBytes("h"), Bytes.toBytes(jsonValue));
    return put;
  }

  private String getJsonValue(EntityType entityType) {
    String value = "";
    switch (entityType) {
      case APPLICATION:
        value = "{\n" +
          "  \"namespacedEntityId\": {\n" +
          "    \"type\": \"application\",\n" +
          "    \"id\": {\n" +
          "      \"namespace\": {\n" +
          "        \"id\": \"ns1\"\n" +
          "      },\n" +
          "      \"applicationId\": \"app1\"\n" +
          "    }\n" +
          "  },\n" +
          "  \"properties\": {\n" +
          "    \"akey1\" : \"avalue1\"\n" +
          "  },\n" +
          "  \"tags\": [\n" +
          "    \"cdap-data-pipeline\"\n" +
          "  ]\n" +
          "}";
        break;
      case DATASET:
        value = "{\n" +
          "  \"namespacedEntityId\": {\n" +
          "    \"type\": \"datasetinstance\",\n" +
          "    \"id\": {\n" +
          "      \"namespace\": {\n" +
          "        \"id\": \"ns1\"\n" +
          "      },\n" +
          "      \"instanceId\": \"ds1\"\n" +
          "    }\n" +
          "  },\n" +
          "  \"properties\": {\n" +
          "    \"akey3\" : \"avalue3\"\n" +
          "  },\n" +
          "  \"tags\": [\n" +
          "    \"explore\",\n" +
          "    \"batch\"\n" +
          "  ]\n" +
          "}";
        break;
      case ARTIFACT:
        value = "{\n" +
          "  \"namespacedEntityId\": {\n" +
          "    \"type\": \"artifact\",\n" +
          "    \"id\": {\n" +
          "      \"namespace\": {\n" +
          "        \"id\": \"ns1\"\n" +
          "      },\n" +
          "      \"name\": \"a1\",\n" +
          "      \"version\": {\n" +
          "        \"version\": \"1.0.0\",\n" +
          "        \"major\": 3,\n" +
          "        \"minor\": 0,\n" +
          "        \"fix\": 4\n" +
          "      }\n" +
          "    }\n" +
          "  },\n" +
          "  \"properties\": {\n" +
          "    \"akey1\" : \"avalue1\"\n" +
          "  },\n" +
          "  \"tags\": []\n" +
          "}";
        break;
      case STREAM:
        value = "{\n" +
          "  \"namespacedEntityId\": {\n" +
          "    \"type\": \"stream\",\n" +
          "    \"id\": {\n" +
          "      \"namespace\": {\n" +
          "        \"id\": \"ns1\"\n" +
          "      },\n" +
          "      \"streamName\": \"s1\"\n" +
          "    }\n" +
          "  },\n" +
          "  \"properties\": {\n" +
          "    \"akey4\" : \"avalue4\"\n" +
          "  },\n" +
          "  \"tags\": []\n" +
          "}";
        break;
    }

    return value;
  }

  private MetadataV1 getMetadataV1(NamespacedEntityId targetId) {
    Map<String, String> properties = ImmutableMap.of("pk1", "pv1", "pk2", "pv2");
    Set<String> tags = ImmutableSet.of("tag1, tag2");
    return new MetadataV1(targetId, properties, tags);
  }

  /**
   * Gets underlying Indexed Table.
   */
  private IndexedTable getIndexedTable(DatasetContext context, DatasetId datasetId) throws Exception {
    String prefix = datasetId.getDataset().contains("business") ? "business" : "system";
    return DatasetsUtil.getOrCreateDataset(context, datasetFramework,
                                           NamespaceId.SYSTEM.dataset(prefix + ".metadata.metadata_index"),
                                           IndexedTable.class.getName(),
                                           DatasetProperties.builder()
                                             .add(IndexedTable.INDEX_COLUMNS_CONF_KEY, "i,n,in,c,ic").build());
  }

  /**
   * Gets metadata table.
   */
  private MetadataDataset getMetadataDataset(DatasetContext context, DatasetId datasetId) throws Exception {
    MetadataScope scope = datasetId.getDataset().contains("business") ? MetadataScope.USER : MetadataScope.SYSTEM;

    return DatasetsUtil.getOrCreateDataset(context, datasetFramework, datasetId, MetadataDataset.class.getName(),
                                           DatasetProperties.builder()
                                             .add(MetadataDatasetDefinition.SCOPE_KEY, scope.name()).build());
  }

  private Injector getInjector() {
    return Guice.createInjector(new ConfigModule(CConfiguration.create(), new Configuration()),
                                new DataSetServiceModules().getInMemoryModules(),
                                new DataSetsModules().getInMemoryModules(),
                                new DataFabricModules().getInMemoryModules(),
                                new ExploreClientModule(),
                                new DiscoveryRuntimeModule().getInMemoryModules(),
                                new LocationRuntimeModule().getInMemoryModules(),
                                new NamespaceClientRuntimeModule().getInMemoryModules(),
                                new NamespaceStoreModule().getStandaloneModules(),
                                new AuthorizationTestModule(),
                                new AuthorizationEnforcementModule().getInMemoryModules(),
                                new AuthenticationContextModules().getMasterModule(),
                                new AbstractModule() {
                                  @Override
                                  protected void configure() {
                                    bind(MetricsCollectionService.class)
                                      .to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
                                    bind(Store.class).to(DefaultStore.class);
                                    bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
                                    bind(OwnerAdmin.class).to(NoOpOwnerAdmin.class);
                                  }
                                });
  }
}
