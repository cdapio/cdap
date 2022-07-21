/*
 * Copyright © 2014-2022 Cask Data, Inc.
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

package io.cdap.cdap.explore.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
import io.cdap.cdap.explore.client.DiscoveryExploreClient;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.explore.guice.ExploreRuntimeModule;
import io.cdap.cdap.explore.service.datasets.KeyStructValueTableDefinition;
import io.cdap.cdap.explore.service.datasets.NotRecordScannableTableDefinition;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.impersonation.UnsupportedUGIProvider;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 * Test deployment behavior when explore module is disabled.
 */
public class ExploreDisabledTest {
  private static final NamespaceId namespaceId = new NamespaceId("myspace");

  private static TransactionManager transactionManager;
  private static DatasetFramework datasetFramework;
  private static DatasetOpExecutorService dsOpExecutor;
  private static DatasetService datasetService;
  private static ExploreClient exploreClient;
  private static NamespaceAdmin namespaceAdmin;

  @BeforeClass
  public static void start() throws Exception {
    Injector injector = Guice.createInjector(createInMemoryModules(CConfiguration.create(), new Configuration()));
    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();

    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));

    dsOpExecutor = injector.getInstance(DatasetOpExecutorService.class);
    dsOpExecutor.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    exploreClient = injector.getInstance(DiscoveryExploreClient.class);
    try {
      exploreClient.ping();
      Assert.fail("Expected not to be able to ping explore client.");
    } catch (ServiceUnavailableException e) {
      // expected
    }

    datasetFramework = injector.getInstance(DatasetFramework.class);

    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    NamespacePathLocator namespacePathLocator = injector.getInstance(NamespacePathLocator.class);

    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder().setName(namespaceId).build();
    namespaceAdmin.create(namespaceMeta);
    // This happens when you create a namespace via REST APIs. However, since we do not start AppFabricServer in
    // Explore tests, simulating that scenario by explicitly calling DatasetFramework APIs.
    namespacePathLocator.get(namespaceId).mkdirs();
    exploreClient.addNamespace(namespaceMeta);
  }

  @AfterClass
  public static void stop() throws Exception {
    exploreClient.removeNamespace(namespaceId);
    namespaceAdmin.delete(namespaceId);
    exploreClient.close();
    datasetService.stopAndWait();
    dsOpExecutor.stopAndWait();
    transactionManager.stopAndWait();
  }

  @Test
  public void testDeployRecordScannable() throws Exception {
    // Try to deploy a dataset that is not record scannable, when explore is enabled.
    // This should be processed with no exception being thrown
    DatasetModuleId module1 = new DatasetModuleId(namespaceId.getNamespace(), "module1");
    DatasetId instance1 = namespaceId.dataset("table1");
    datasetFramework.addModule(module1, new KeyStructValueTableDefinition.KeyStructValueTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", instance1, DatasetProperties.EMPTY);

    Transaction tx1 = transactionManager.startShort(100);

    // Accessing dataset instance to perform data operations
    KeyStructValueTableDefinition.KeyStructValueTable table =
      datasetFramework.getDataset(instance1, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);
    table.startTx(tx1);

    KeyStructValueTableDefinition.KeyValue.Value value1 =
        new KeyStructValueTableDefinition.KeyValue.Value("first", Lists.newArrayList(1, 2, 3, 4, 5));
    KeyStructValueTableDefinition.KeyValue.Value value2 =
        new KeyStructValueTableDefinition.KeyValue.Value("two", Lists.newArrayList(10, 11, 12, 13, 14));
    table.put("1", value1);
    table.put("2", value2);
    Assert.assertEquals(value1, table.get("1"));

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx1.getTransactionId(), table.getTxChanges());
    transactionManager.commit(tx1.getTransactionId(), tx1.getWritePointer());

    table.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    table.startTx(tx2);

    Assert.assertEquals(value1, table.get("1"));

    datasetFramework.deleteInstance(instance1);
    datasetFramework.deleteModule(module1);
  }

  @Test
  public void testDeployNotRecordScannable() throws Exception {
    // Try to deploy a dataset that is not record scannable, when explore is enabled.
    // This should be processed with no exceptionbeing thrown
    DatasetModuleId module2 = namespaceId.datasetModule("module2");
    DatasetId instance2 = namespaceId.dataset("table1");
    datasetFramework.addModule(module2, new NotRecordScannableTableDefinition.NotRecordScannableTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("NotRecordScannableTableDef", instance2, DatasetProperties.EMPTY);

    Transaction tx1 = transactionManager.startShort(100);

    // Accessing dataset instance to perform data operations
    NotRecordScannableTableDefinition.KeyValueTable table =
      datasetFramework.getDataset(instance2, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);
    table.startTx(tx1);

    table.write("key1", "value1");
    table.write("key2", "value2");
    byte[] value = table.read("key1");
    Assert.assertEquals("value1", Bytes.toString(value));

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx1.getTransactionId(), table.getTxChanges());
    transactionManager.commit(tx1.getTransactionId(), tx1.getWritePointer());

    table.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    table.startTx(tx2);

    value = table.read("key1");
    Assert.assertNotNull(value);
    Assert.assertEquals("value1", Bytes.toString(value));

    datasetFramework.deleteInstance(instance2);
    datasetFramework.deleteModule(module2);
  }

  private static List<Module> createInMemoryModules(CConfiguration configuration, Configuration hConf) {
    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.MEMORY.name());
    configuration.setBoolean(Constants.Explore.EXPLORE_ENABLED, false);
    configuration.set(Constants.Explore.LOCAL_DATA_DIR,
                      new File(System.getProperty("java.io.tmpdir"), "hive").getAbsolutePath());

    return ImmutableList.of(
        new ConfigModule(configuration, hConf),
        RemoteAuthenticatorModules.getNoOpModule(),
        new IOModule(),
        new InMemoryDiscoveryModule(),
        new NonCustomLocationUnitTestModule(),
        new DataFabricModules().getInMemoryModules(),
        new DataSetsModules().getStandaloneModules(),
        new DataSetServiceModules().getInMemoryModules(),
        new MetricsClientRuntimeModule().getInMemoryModules(),
        new ExploreRuntimeModule().getInMemoryModules(),
        new ExploreClientModule(),
        new NamespaceAdminTestModule(),
        new AuthorizationTestModule(),
        new AuthorizationEnforcementModule().getInMemoryModules(),
        new AuthenticationContextModules().getMasterModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
            bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
            bind(MetadataServiceClient.class).to(NoOpMetadataServiceClient.class);
          }
        }
    );
  }
}
