package com.continuuity.data2.datafabric.dataset;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.Networks;
import com.continuuity.data2.datafabric.dataset.client.DatasetManagerServiceClient;
import com.continuuity.data2.datafabric.dataset.service.DatasetManagerService;
import com.continuuity.data2.dataset2.manager.AbstractDatasetManagerTest;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.dataset2.manager.inmemory.InMemoryDatasetDefinitionRegistry;
import com.continuuity.data2.dataset2.manager.inmemory.InMemoryDatasetManager;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.internal.data.dataset.module.DatasetModule;
import com.google.common.collect.ImmutableSortedMap;
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
public class DataFabricDatasetManagerTest extends AbstractDatasetManagerTest {
  private DatasetManagerService datasetManager;
  private DatasetManager manager;

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

    // Starting DatasetManagerService service
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();

    // Tx Manager to support working with datasets
    InMemoryTransactionManager txManager = new InMemoryTransactionManager();
    txManager.startAndWait();
    InMemoryTxSystemClient txSystemClient = new InMemoryTxSystemClient(txManager);

    datasetManager = new DatasetManagerService(cConf,
                                               new LocalLocationFactory(),
                                               discoveryService,
                                               new InMemoryDatasetManager(),
                                               ImmutableSortedMap.<String, Class<? extends DatasetModule>>of(
                                                 "memoryTable", InMemoryTableModule.class),
                                               txSystemClient);
    datasetManager.startAndWait();

    DatasetManagerServiceClient dsManagerClient = new DatasetManagerServiceClient(discoveryService);
    manager = new DataFabricDatasetManager(dsManagerClient, cConf,
                                           new LocalLocationFactory(),
                                           new InMemoryDatasetDefinitionRegistry());
  }

  @After
  public void after() {
    datasetManager.stopAndWait();
  }

  @Override
  protected DatasetManager getDatasetManager() {
    return manager;
  }
}
