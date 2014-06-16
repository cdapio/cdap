package com.continuuity.data2.datafabric.dataset;

import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.NoOpMetricsCollectionService;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.common.utils.Networks;
import com.continuuity.data2.datafabric.dataset.client.DatasetServiceClient;
import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.data2.datafabric.dataset.service.executor.InMemoryDatasetOpExecutor;
import com.continuuity.data2.dataset2.AbstractDatasetFrameworkTest;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.InMemoryDatasetFramework;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryOrderedTableModule;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;

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
    cConf.setInt(Constants.Dataset.Manager.PORT, Networks.getRandomPort());

    // Starting DatasetService service
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    MetricsCollectionService metricsCollectionService = new NoOpMetricsCollectionService();

    // Tx Manager to support working with datasets
    InMemoryTransactionManager txManager = new InMemoryTransactionManager();
    txManager.startAndWait();
    InMemoryTxSystemClient txSystemClient = new InMemoryTxSystemClient(txManager);

    DatasetServiceClient dsManagerClient = new DatasetServiceClient(discoveryService);
    framework = new RemoteDatasetFramework(dsManagerClient, cConf,
                                           new LocalLocationFactory(),
                                           new InMemoryDefinitionRegistryFactory());
    InMemoryDatasetOpExecutor opExecutorClient = new InMemoryDatasetOpExecutor(framework);

    service = new DatasetService(cConf,
                                 new LocalLocationFactory(),
                                 discoveryService,
                                 new InMemoryDatasetFramework(),
                                 ImmutableMap.<String, DatasetModule>of("memoryTable",
                                                                        new InMemoryOrderedTableModule()),
                                 txSystemClient,
                                 metricsCollectionService,
                                 opExecutorClient);
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
