/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.CConfigurationUtil;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpHTTPHandler;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.InMemoryDatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.service.mds.MDSDatasetsRegistry;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.metrics.DatasetMetricsReporter;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.explore.client.DiscoveryExploreClient;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.ObjectResponse;
import co.cask.http.HttpHandler;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import co.cask.tephra.runtime.TransactionInMemoryModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.common.Services;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Base class for unit-tests that require running of {@link DatasetService}
 */
public abstract class DatasetServiceTestBase {
  private InMemoryDiscoveryService discoveryService;
  private DatasetOpExecutorService opExecutorService;
  private DatasetService service;
  private LocationFactory locationFactory;
  protected TransactionManager txManager;
  protected RemoteDatasetFramework dsFramework;

  private int port = -1;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    File dataDir = new File(tmpFolder.newFolder(), "data");
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, dataDir.getAbsolutePath());
    if (!DirUtils.mkdirs(dataDir)) {
      throw new RuntimeException(String.format("Could not create DatasetFramework output dir %s", dataDir));
    }
    cConf.set(Constants.Dataset.Manager.OUTPUT_DIR, dataDir.getAbsolutePath());
    cConf.set(Constants.Dataset.Manager.ADDRESS, "localhost");
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);

    // Starting DatasetService service
    discoveryService = new InMemoryDiscoveryService();
    MetricsCollectionService metricsCollectionService = new NoOpMetricsCollectionService();

    // Tx Manager to support working with datasets
    Configuration txConf = HBaseConfiguration.create();
    CConfigurationUtil.copyTxProperties(cConf, txConf);
    txManager = new TransactionManager(txConf);
    txManager.startAndWait();
    InMemoryTxSystemClient txSystemClient = new InMemoryTxSystemClient(txManager);

    final Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocationRuntimeModule().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new TransactionInMemoryModule());

    DatasetDefinitionRegistryFactory registryFactory = new DatasetDefinitionRegistryFactory() {
      @Override
      public DatasetDefinitionRegistry create() {
        DefaultDatasetDefinitionRegistry registry = new DefaultDatasetDefinitionRegistry();
        injector.injectMembers(registry);
        return registry;
      }
    };

    locationFactory = injector.getInstance(LocationFactory.class);
    NamespacedLocationFactory namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);
    dsFramework = new RemoteDatasetFramework(discoveryService, registryFactory);

    ImmutableSet<HttpHandler> handlers =
      ImmutableSet.<HttpHandler>of(new DatasetAdminOpHTTPHandler(dsFramework, cConf, locationFactory));
    opExecutorService = new DatasetOpExecutorService(cConf, discoveryService, metricsCollectionService, handlers);

    opExecutorService.startAndWait();

    ImmutableMap<String, DatasetModule> modules = ImmutableMap.<String, DatasetModule>builder()
      .putAll(injector.getInstance(Key.get(new TypeLiteral<Map<String, DatasetModule>>() { },
                                           Names.named("defaultDatasetModules"))))
      .putAll(DatasetMetaTableUtil.getModules())
      .build();

    TransactionExecutorFactory txExecutorFactory = injector.getInstance(TransactionExecutorFactory.class);

    MDSDatasetsRegistry mdsDatasetsRegistry =
      new MDSDatasetsRegistry(txSystemClient, new InMemoryDatasetFramework(registryFactory, modules, cConf));

    ExploreFacade exploreFacade = new ExploreFacade(new DiscoveryExploreClient(discoveryService), cConf);
    service = new DatasetService(cConf,
                                 namespacedLocationFactory,
                                 discoveryService,
                                 discoveryService,
                                 new DatasetTypeManager(cConf, mdsDatasetsRegistry, locationFactory,
                                                        // we don't need any default modules in this test
                                                        Collections.<String, DatasetModule>emptyMap()),
                                 new DatasetInstanceManager(mdsDatasetsRegistry),
                                 metricsCollectionService,
                                 new InMemoryDatasetOpExecutor(dsFramework),
                                 mdsDatasetsRegistry,
                                 exploreFacade,
                                 new HashSet<DatasetMetricsReporter>(),
                                 new LocalStorageProviderNamespaceAdmin(cConf, namespacedLocationFactory,
                                                                        exploreFacade),
                                 new UsageRegistry(txExecutorFactory, dsFramework));

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
    // this usually happens while creating a namespace, however not doing that in data fabric tests
    Locations.mkdirsIfNotExists(namespacedLocationFactory.get(Constants.DEFAULT_NAMESPACE_ID));
  }

  @After
  public void after() {
    Services.chainStop(service, opExecutorService, txManager);
    Locations.deleteQuietly(locationFactory.create(Constants.DEFAULT_NAMESPACE));
  }

  private synchronized int getPort() {
    int attempts = 0;
    while (port < 0 && attempts++ < 10) {
      ServiceDiscovered discovered = discoveryService.discover(Constants.Service.DATASET_MANAGER);
      if (!discovered.iterator().hasNext()) {
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        continue;
      }
      port = discovered.iterator().next().getSocketAddress().getPort();
    }

    return port;
  }

  protected URL getUrl(String path) throws MalformedURLException {
    return new URL(String.format("http://localhost:%d/%s/namespaces/%s%s",
                                 getPort(), Constants.Gateway.API_VERSION_3_TOKEN, Constants.DEFAULT_NAMESPACE, path));
  }

  protected URL getUnderlyingNamespaceAdminUrl(String namespace, String operation) throws MalformedURLException {
    String resource = String.format("%s/namespaces/%s/data/admin/%s",
                                    Constants.Gateway.API_VERSION_3, namespace, operation);
    return new URL("http://" + "localhost" + ":" + getPort() + resource);
  }

  protected Location createModuleJar(Class moduleClass, Location...bundleEmbeddedJars) throws IOException {
    LocationFactory lf = new LocalLocationFactory(tmpFolder.newFolder());
    File[] embeddedJars = new File[bundleEmbeddedJars.length];
    for (int i = 0; i < bundleEmbeddedJars.length; i++) {
      File file = tmpFolder.newFile();
      Files.copy(Locations.newInputSupplier(bundleEmbeddedJars[i]), file);
      embeddedJars[i] = file;
    }

    return AppJarHelper.createDeploymentJar(lf, moduleClass, embeddedJars);
  }

  protected int deployModule(String moduleName, Class moduleClass) throws Exception {
    Location moduleJar = createModuleJar(moduleClass);
    HttpRequest request = HttpRequest.put(getUrl("/data/modules/" + moduleName))
      .addHeader("X-Class-Name", moduleClass.getName())
      .withBody(Locations.newInputSupplier(moduleJar)).build();
    return HttpRequests.execute(request).getResponseCode();
  }

  // creates a bundled jar with moduleClass and list of bundleEmbeddedJar files, moduleName and moduleClassName are
  // used to make request for deploying module.
  protected int deployModuleBundled(String moduleName, String moduleClassName, Class moduleClass,
                                    Location...bundleEmbeddedJars) throws IOException {
    Location moduleJar = createModuleJar(moduleClass, bundleEmbeddedJars);
    HttpRequest request = HttpRequest.put(getUrl("/data/modules/" + moduleName))
      .addHeader("X-Class-Name", moduleClassName)
      .withBody(Locations.newInputSupplier(moduleJar)).build();
    return HttpRequests.execute(request).getResponseCode();
  }

  protected ObjectResponse<List<DatasetModuleMeta>> getModules() throws IOException {
    return ObjectResponse.fromJsonBody(HttpRequests.execute(HttpRequest.get(getUrl("/data/modules")).build()),
                                       new TypeToken<List<DatasetModuleMeta>>() { }.getType());
  }

  protected int deleteModule(String moduleName) throws Exception {
    return HttpRequests.execute(HttpRequest.delete(getUrl("/data/modules/" + moduleName)).build()).getResponseCode();
  }

  protected int deleteModules() throws IOException {
    return HttpRequests.execute(HttpRequest.delete(getUrl("/data/modules/")).build()).getResponseCode();
  }
}
