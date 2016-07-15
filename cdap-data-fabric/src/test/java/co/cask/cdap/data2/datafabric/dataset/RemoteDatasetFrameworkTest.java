/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfigurationUtil;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.NamespacedLocationFactoryTestClient;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data.runtime.DynamicTransactionExecutorFactory;
import co.cask.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetInstanceService;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.LocalStorageProviderNamespaceAdmin;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpHTTPHandler;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.InMemoryDatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.service.executor.LocalDatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.data2.dataset2.AbstractDatasetFrameworkTest;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.SimpleKVTable;
import co.cask.cdap.data2.dataset2.SingleTypeModule;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import co.cask.cdap.data2.metadata.store.NoOpMetadataStore;
import co.cask.cdap.data2.metrics.DatasetMetricsReporter;
import co.cask.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.data2.transaction.TransactionSystemClientService;
import co.cask.cdap.explore.client.DiscoveryExploreClient;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.http.HttpHandler;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.internal.Services;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link RemoteDatasetFramework}
 */
public class RemoteDatasetFrameworkTest extends AbstractDatasetFrameworkTest {
  private TransactionManager txManager;
  private DatasetOpExecutorService opExecutorService;
  private DatasetService service;
  private RemoteDatasetFramework framework;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    File dataDir = new File(tmpFolder.newFolder(), "data");
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, dataDir.getAbsolutePath());
    cConf.set(Constants.Dataset.Manager.ADDRESS, "localhost");
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);

    // Starting DatasetService service
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    MetricsCollectionService metricsCollectionService = new NoOpMetricsCollectionService();

    // Tx Manager to support working with datasets
    Configuration txConf = HBaseConfiguration.create();
    CConfigurationUtil.copyTxProperties(cConf, txConf);
    txManager = new TransactionManager(txConf);
    txManager.startAndWait();
    InMemoryTxSystemClient txSystemClient = new InMemoryTxSystemClient(txManager);
    TransactionSystemClientService txSystemClientService = new DelegatingTransactionSystemClientService(txSystemClient);

    LocalLocationFactory locationFactory = new LocalLocationFactory(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR)));
    NamespacedLocationFactory namespacedLocationFactory = new NamespacedLocationFactoryTestClient(cConf,
                                                                                                  locationFactory);
    framework = new RemoteDatasetFramework(cConf, discoveryService, registryFactory);
    SystemDatasetInstantiatorFactory datasetInstantiatorFactory =
      new SystemDatasetInstantiatorFactory(locationFactory, framework, cConf);

    DatasetAdminService datasetAdminService =
      new DatasetAdminService(framework, cConf, locationFactory, datasetInstantiatorFactory, new NoOpMetadataStore());
    ImmutableSet<HttpHandler> handlers =
      ImmutableSet.<HttpHandler>of(new DatasetAdminOpHTTPHandler(datasetAdminService));
    opExecutorService = new DatasetOpExecutorService(cConf, discoveryService, metricsCollectionService, handlers);
    opExecutorService.startAndWait();

    ImmutableMap<String, DatasetModule> modules = ImmutableMap.<String, DatasetModule>builder()
      .put("memoryTable", new InMemoryTableModule())
      .put("core", new CoreDatasetsModule())
      .putAll(DatasetMetaTableUtil.getModules())
      .build();

    InMemoryDatasetFramework mdsFramework = new InMemoryDatasetFramework(registryFactory, modules);

    ExploreFacade exploreFacade = new ExploreFacade(new DiscoveryExploreClient(cConf, discoveryService), cConf);
    TransactionExecutorFactory txExecutorFactory = new DynamicTransactionExecutorFactory(txSystemClient);
    DatasetInstanceService instanceService = new DatasetInstanceService(
      new DatasetTypeManager(cConf, locationFactory, txSystemClientService,
                             txExecutorFactory, mdsFramework, DEFAULT_MODULES),
      new DatasetInstanceManager(txSystemClientService, txExecutorFactory, mdsFramework),
      new LocalDatasetOpExecutor(cConf, discoveryService, opExecutorService),
      exploreFacade,
      cConf,
      namespaceQueryAdmin);
    instanceService.setAuditPublisher(inMemoryAuditPublisher);

    service = new DatasetService(cConf,
                                 namespacedLocationFactory,
                                 discoveryService,
                                 discoveryService,
                                 new DatasetTypeManager(cConf, locationFactory, txSystemClientService,
                                                        txExecutorFactory, mdsFramework, DEFAULT_MODULES),
                                 metricsCollectionService,
                                 new InMemoryDatasetOpExecutor(framework),
                                 new HashSet<DatasetMetricsReporter>(),
                                 instanceService,
                                 new LocalStorageProviderNamespaceAdmin(cConf, namespacedLocationFactory,
                                                                        exploreFacade, namespaceQueryAdmin),
                                 namespaceQueryAdmin
    );
    // Start dataset service, wait for it to be discoverable
    service.start();
    final CountDownLatch startLatch = new CountDownLatch(1);
    discoveryService.discover(Constants.Service.DATASET_MANAGER).watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        if (!Iterables.isEmpty(serviceDiscovered)) {
          startLatch.countDown();
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    startLatch.await(5, TimeUnit.SECONDS);


    // the framework.delete looks up namespace config through namespaceadmin add the meta there too.
    NamespaceMeta systemMeta = new NamespaceMeta.Builder().setName(Id.Namespace.SYSTEM).build();
    framework.createNamespace(systemMeta);
    namespaceAdmin.create(systemMeta);
    NamespaceMeta name = new NamespaceMeta.Builder().setName(NAMESPACE_ID).build();
    framework.createNamespace(name);
    namespaceAdmin.create(name);
  }

  // Note: Cannot have these system namespace restrictions in system namespace since we use it internally in
  // DatasetMetaTable util to add modules to system namespace. However, we should definitely impose these restrictions
  // in RemoteDatasetFramework.
  @Test
  public void testSystemNamespace() throws DatasetManagementException {
    DatasetFramework framework = getFramework();
    // Adding module to system namespace should fail
    try {
      framework.addModule(Id.DatasetModule.from(Id.Namespace.SYSTEM, "keyValue"),
                          new SingleTypeModule(SimpleKVTable.class));
      Assert.fail("Should not be able to add a module to system namespace");
    } catch (DatasetManagementException e) {
      // expected
    }
    try {
      framework.deleteModule(Id.DatasetModule.from(Id.Namespace.SYSTEM, "orderedTable-memory"));
      Assert.fail("Should not be able to delete a default module.");
    } catch (DatasetManagementException e) {
      // expected
    }
    try {
      framework.deleteAllModules(Id.Namespace.SYSTEM);
      Assert.fail("Should not be able to delete modules from system namespace");
    } catch (DatasetManagementException e) {
      // expected
    }
  }

  @After
  public void after() throws Exception {
    // since we stored namespace meta through admin so that framework.delete can lookup namespaceconfig clean the
    // meta from there too
    framework.deleteNamespace(NAMESPACE_ID);
    namespaceAdmin.delete(NAMESPACE_ID);
    framework.deleteNamespace(Id.Namespace.SYSTEM);
    namespaceAdmin.delete(Id.Namespace.SYSTEM);
    Futures.getUnchecked(Services.chainStop(service, opExecutorService, txManager));
  }

  @Override
  protected DatasetFramework getFramework() {
    return framework;
  }
}
