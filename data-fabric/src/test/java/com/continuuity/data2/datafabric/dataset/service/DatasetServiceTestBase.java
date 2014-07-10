package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.HttpRequests;
import com.continuuity.common.http.ObjectResponse;
import com.continuuity.common.lang.jar.JarFinder;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.NoOpMetricsCollectionService;
import com.continuuity.data2.datafabric.dataset.InMemoryDefinitionRegistryFactory;
import com.continuuity.data2.datafabric.dataset.RemoteDatasetFramework;
import com.continuuity.data2.datafabric.dataset.instance.DatasetInstanceManager;
import com.continuuity.data2.datafabric.dataset.service.executor.InMemoryDatasetOpExecutor;
import com.continuuity.data2.datafabric.dataset.service.mds.MDSDatasetsRegistry;
import com.continuuity.data2.datafabric.dataset.type.DatasetModuleMeta;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeManager;
import com.continuuity.data2.datafabric.dataset.type.LocalDatasetTypeClassLoaderFactory;
import com.continuuity.data2.dataset2.InMemoryDatasetFramework;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryOrderedTableModule;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.explore.client.DatasetExploreFacade;
import com.continuuity.explore.client.DiscoveryExploreClient;
import com.google.common.collect.ImmutableMap;
import com.google.gson.reflect.TypeToken;
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
import java.util.Collections;
import java.util.List;

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
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);

    // Starting DatasetService service
    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    MetricsCollectionService metricsCollectionService = new NoOpMetricsCollectionService();

    // Tx Manager to support working with datasets
    txManager = new InMemoryTransactionManager();
    txManager.startAndWait();
    InMemoryTxSystemClient txSystemClient = new InMemoryTxSystemClient(txManager);

    LocalLocationFactory locationFactory = new LocalLocationFactory();
    dsFramework = new RemoteDatasetFramework(discoveryService, locationFactory, new InMemoryDefinitionRegistryFactory(),
                                             new LocalDatasetTypeClassLoaderFactory());

    ImmutableMap<String, ? extends DatasetModule> defaultModules =
      ImmutableMap.of("memoryTable", new InMemoryOrderedTableModule());

    MDSDatasetsRegistry mdsDatasetsRegistry =
      new MDSDatasetsRegistry(txSystemClient, defaultModules,
                              new InMemoryDatasetFramework(new InMemoryDefinitionRegistryFactory()), cConf);

    service = new DatasetService(cConf,
                                 locationFactory,
                                 discoveryService,
                                 new DatasetTypeManager(mdsDatasetsRegistry, locationFactory,
                                                        // we don't need any default modules in this test
                                                        Collections.<String, DatasetModule>emptyMap()),
                                 new DatasetInstanceManager(mdsDatasetsRegistry),
                                 metricsCollectionService,
                                 new InMemoryDatasetOpExecutor(dsFramework),
                                 mdsDatasetsRegistry,
                                 new DatasetExploreFacade(new DiscoveryExploreClient(discoveryService), cConf));
    service.startAndWait();
    port = discoveryService.discover(Constants.Service.DATASET_MANAGER).iterator().next().getSocketAddress().getPort();
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
                                    (byte[]) null, is).getResponseCode();
    } finally {
      is.close();
    }
  }

  protected ObjectResponse<List<DatasetModuleMeta>> getModules() throws IOException {
    return ObjectResponse.fromJsonBody(HttpRequests.get(getUrl("/data/modules")),
                                       new TypeToken<List<DatasetModuleMeta>>() {
                                       }.getType());
  }

  protected int deleteInstances() throws IOException {
    return HttpRequests.delete(getUrl("/data/unrecoverable/datasets")).getResponseCode();
  }

  protected int deleteModule(String moduleName) throws Exception {
    return HttpRequests.delete(getUrl("/data/modules/" + moduleName)).getResponseCode();
  }

  protected int deleteModules() throws IOException {
    return HttpRequests.delete(getUrl("/data/modules/")).getResponseCode();
  }
}
