package com.continuuity.data2.datafabric.dataset;

import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.NoOpMetricsCollectionService;
import com.continuuity.data2.datafabric.dataset.instance.DatasetInstanceManager;
import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.data2.datafabric.dataset.service.executor.InMemoryDatasetOpExecutor;
import com.continuuity.data2.datafabric.dataset.service.mds.MDSDatasetsRegistry;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeManager;
import com.continuuity.data2.datafabric.dataset.type.LocalDatasetTypeClassLoaderFactory;
import com.continuuity.data2.dataset2.AbstractDatasetFrameworkTest;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.InMemoryDatasetFramework;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryOrderedTableModule;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.explore.client.DatasetExploreFacade;
import com.continuuity.explore.client.DiscoveryExploreClient;

import com.google.common.collect.ImmutableMap;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;

/**
 *
 */
public class RemoteDatasetFrameworkTest extends AbstractDatasetFrameworkTest {
  private DatasetService service;
  private RemoteDatasetFramework framework;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    File datasetDir = new File(tmpFolder.newFolder(), "dataset");
    datasetDir.mkdirs();
    cConf.set(Constants.Dataset.Manager.OUTPUT_DIR, datasetDir.getAbsolutePath());
    cConf.set(Constants.Dataset.Manager.ADDRESS, "localhost");
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);

    // Starting DatasetService service
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    MetricsCollectionService metricsCollectionService = new NoOpMetricsCollectionService();

    // Tx Manager to support working with datasets
    InMemoryTransactionManager txManager = new InMemoryTransactionManager();
    txManager.startAndWait();
    InMemoryTxSystemClient txSystemClient = new InMemoryTxSystemClient(txManager);

    LocalLocationFactory locationFactory = new LocalLocationFactory();
    framework = new RemoteDatasetFramework(discoveryService, locationFactory, new InMemoryDefinitionRegistryFactory(),
                                             new LocalDatasetTypeClassLoaderFactory());

    MDSDatasetsRegistry mdsDatasetsRegistry =
      new MDSDatasetsRegistry(txSystemClient,
                              ImmutableMap.of("memoryTable", new InMemoryOrderedTableModule()),
                              new InMemoryDatasetFramework(new InMemoryDefinitionRegistryFactory()), cConf);

    service = new DatasetService(cConf,
                                 locationFactory,
                                 discoveryService,
                                 new DatasetTypeManager(mdsDatasetsRegistry, locationFactory,
                                                        // note: in this test we start with empty modules
                                                        Collections.<String, DatasetModule>emptyMap()),
                                 new DatasetInstanceManager(mdsDatasetsRegistry),
                                 metricsCollectionService,
                                 new InMemoryDatasetOpExecutor(framework),
                                 mdsDatasetsRegistry,
                                 new DatasetExploreFacade(new DiscoveryExploreClient(discoveryService), cConf));
    service.startAndWait();
  }

  @After
  public void after() {
    service.stopAndWait();
  }

  @Override
  protected DatasetFramework getFramework() {
    return framework;
  }
}
