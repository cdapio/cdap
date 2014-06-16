package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.HttpRequests;
import com.continuuity.common.lang.jar.JarFinder;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.NoOpMetricsCollectionService;
import com.continuuity.common.utils.Networks;
import com.continuuity.data2.datafabric.dataset.InMemoryDefinitionRegistryFactory;
import com.continuuity.data2.datafabric.dataset.RemoteDatasetFramework;
import com.continuuity.data2.datafabric.dataset.client.DatasetServiceClient;
import com.continuuity.data2.datafabric.dataset.service.executor.InMemoryDatasetOpExecutor;
import com.continuuity.data2.dataset2.InMemoryDatasetFramework;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryOrderedTableModule;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Base class for unit-tests that require running of {@link DatasetService}
 */
public abstract class DatasetServiceTestBase {
  private int port;
  private DatasetService service;
  protected InMemoryTransactionManager txManager;
  protected RemoteDatasetFramework dsFramework;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    File datasetDir = new File(tmpFolder.newFolder(), "dataset");
    if (!datasetDir.mkdirs()) {
      throw
        new RuntimeException(String.format("Could not create DatasetFramework output dir %s", datasetDir.getPath()));
    }
    cConf.set(Constants.Dataset.Manager.OUTPUT_DIR, datasetDir.getAbsolutePath());
    cConf.set(Constants.Dataset.Manager.ADDRESS, "localhost");
    port = Networks.getRandomPort();
    cConf.setInt(Constants.Dataset.Manager.PORT, port);

    // Starting DatasetService service
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    MetricsCollectionService metricsCollectionService = new NoOpMetricsCollectionService();

    // Tx Manager to support working with datasets
    txManager = new InMemoryTransactionManager();
    txManager.startAndWait();
    InMemoryTxSystemClient txSystemClient = new InMemoryTxSystemClient(txManager);

    dsFramework = new RemoteDatasetFramework(new DatasetServiceClient(discoveryService), cConf,
                                             new LocalLocationFactory(), new InMemoryDefinitionRegistryFactory());

    service = new DatasetService(cConf,
                                 new LocalLocationFactory(),
                                 discoveryService,
                                 new InMemoryDatasetFramework(),
                                 ImmutableMap.<String, DatasetModule>of("memoryTable",
                                                                        new InMemoryOrderedTableModule()),
                                 txSystemClient,
                                 metricsCollectionService,
                                 new InMemoryDatasetOpExecutor(dsFramework));
    service.startAndWait();

  }

  @After
  public void after() {
    try {
      service.stopAndWait();
    } finally {
      txManager.stopAndWait();
    }
  }

  protected URL getUrl(String resource) throws MalformedURLException {
    return new URL("http://" + "localhost" + ":" + port + Constants.Gateway.GATEWAY_VERSION + resource);
  }

  protected int deployModule(String moduleName, Class moduleClass) throws Exception {
    String jarPath = JarFinder.getJar(moduleClass);

    FileInputStream is = new FileInputStream(jarPath);
    try {
      return HttpRequests.doRequest("PUT", getUrl("/data/modules/" + moduleName),
                                    ImmutableMap.of("X-Continuuity-Class-Name", moduleClass.getName()),
                                    null, is).getResponseCode();
    } finally {
      is.close();
    }
  }

  protected int deleteModule(String moduleName) throws Exception {
    return HttpRequests.delete(getUrl("/data/modules/" + moduleName)).getResponseCode();
  }

  protected int deleteModules() throws IOException {
    return HttpRequests.delete(getUrl("/data/modules/")).getResponseCode();
  }
}
