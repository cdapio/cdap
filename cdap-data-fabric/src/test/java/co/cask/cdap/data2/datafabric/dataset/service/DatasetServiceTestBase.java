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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data.runtime.DynamicTransactionExecutorFactory;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import co.cask.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpHTTPHandler;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.InMemoryDatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.metadata.store.NoOpMetadataStore;
import co.cask.cdap.data2.metrics.DatasetMetricsReporter;
import co.cask.cdap.data2.security.Impersonator;
import co.cask.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.data2.transaction.TransactionSystemClientService;
import co.cask.cdap.explore.client.DiscoveryExploreClient;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationEnforcementService;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import co.cask.http.HttpHandler;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.runtime.TransactionInMemoryModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.reflect.TypeToken;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.Services;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Base class for unit-tests that require running of {@link DatasetService}
 */
public abstract class DatasetServiceTestBase {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  protected static LocationFactory locationFactory;
  protected static NamespaceAdmin namespaceAdmin;
  protected static TransactionManager txManager;
  protected static RemoteDatasetFramework dsFramework;
  protected static InMemoryDatasetFramework inMemoryDatasetFramework;
  protected static DatasetInstanceService instanceService;
  protected static DatasetDefinitionRegistryFactory registryFactory;
  protected static Injector injector;

  private static AuthorizationEnforcementService authEnforcementService;
  private static DiscoveryServiceClient discoveryServiceClient;
  private static DatasetOpExecutorService opExecutorService;
  private static DatasetService service;

  private int port = -1;

  protected static void initialize() throws Exception {
    locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    initializeAndStartService(createCConf());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Services.chainStop(service, opExecutorService, txManager, authEnforcementService);
    namespaceAdmin.delete(Id.Namespace.DEFAULT);
    Locations.deleteQuietly(locationFactory.create(Id.Namespace.DEFAULT.getId()));
  }

  protected static void initializeAndStartService(CConfiguration cConf) throws Exception {
    // TODO: this whole method is a mess. Streamline it!
    injector = Guice.createInjector(
      new ConfigModule(cConf),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new NonCustomLocationUnitTestModule().getModule(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new TransactionInMemoryModule(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Singleton.class);
          install(new FactoryModuleBuilder()
                    .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                    .build(DatasetDefinitionRegistryFactory.class));
          // through the injector, we only need RemoteDatasetFramework in these tests
          bind(RemoteDatasetFramework.class);
        }
      });

    AuthorizationEnforcer authEnforcer = injector.getInstance(AuthorizationEnforcer.class);
    authEnforcementService = injector.getInstance(AuthorizationEnforcementService.class);
    authEnforcementService.startAndWait();

    AuthenticationContext authenticationContext = injector.getInstance(AuthenticationContext.class);

    DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);
    discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    dsFramework = injector.getInstance(RemoteDatasetFramework.class);
    // Tx Manager to support working with datasets
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    TransactionSystemClient txSystemClient = injector.getInstance(TransactionSystemClient.class);
    TransactionSystemClientService txSystemClientService =
      new DelegatingTransactionSystemClientService(txSystemClient);

    NamespacedLocationFactory namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);
    SystemDatasetInstantiatorFactory datasetInstantiatorFactory =
      new SystemDatasetInstantiatorFactory(locationFactory, dsFramework, cConf);

    // ok to pass null, since the impersonator won't actually be called, if kerberos security is not enabled
    Impersonator impersonator = new Impersonator(cConf, null, null);
    DatasetAdminService datasetAdminService =
      new DatasetAdminService(dsFramework, cConf, locationFactory, datasetInstantiatorFactory, new NoOpMetadataStore(),
                              impersonator);
    ImmutableSet<HttpHandler> handlers =
      ImmutableSet.<HttpHandler>of(new DatasetAdminOpHTTPHandler(datasetAdminService));
    MetricsCollectionService metricsCollectionService = injector.getInstance(MetricsCollectionService.class);

    opExecutorService = new DatasetOpExecutorService(cConf, discoveryService, metricsCollectionService, handlers);
    opExecutorService.startAndWait();

    Map<String, DatasetModule> defaultModules =
      injector.getInstance(Key.get(new TypeLiteral<Map<String, DatasetModule>>() { },
                                   Names.named("defaultDatasetModules")));

    ImmutableMap<String, DatasetModule> modules = ImmutableMap.<String, DatasetModule>builder()
      .putAll(defaultModules)
      .putAll(DatasetMetaTableUtil.getModules())
      .build();

    registryFactory = injector.getInstance(DatasetDefinitionRegistryFactory.class);
    inMemoryDatasetFramework = new InMemoryDatasetFramework(registryFactory, modules);

    ExploreFacade exploreFacade = new ExploreFacade(new DiscoveryExploreClient(cConf, discoveryServiceClient), cConf);
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespaceAdmin.create(NamespaceMeta.DEFAULT);
    NamespaceQueryAdmin namespaceQueryAdmin = injector.getInstance(NamespaceQueryAdmin.class);
    TransactionExecutorFactory txExecutorFactory = new DynamicTransactionExecutorFactory(txSystemClient);
    DatasetTypeManager typeManager = new DatasetTypeManager(cConf, locationFactory, txSystemClientService,
                                                            txExecutorFactory,
                                                            inMemoryDatasetFramework, defaultModules,
                                                            impersonator);
    DatasetOpExecutor opExecutor = new InMemoryDatasetOpExecutor(dsFramework);
    DatasetInstanceManager instanceManager =
      new DatasetInstanceManager(txSystemClientService, txExecutorFactory, inMemoryDatasetFramework);
    PrivilegesManager privilegesManager = injector.getInstance(PrivilegesManager.class);
    instanceService = new DatasetInstanceService(typeManager, instanceManager, opExecutor, exploreFacade,
                                                 namespaceQueryAdmin, authEnforcer, privilegesManager,
                                                 authenticationContext);

    DatasetTypeService typeService = new DatasetTypeService(typeManager, namespaceAdmin, namespacedLocationFactory,
                                                            authEnforcer, privilegesManager, authenticationContext,
                                                            cConf, impersonator);
    service = new DatasetService(cConf, discoveryService, discoveryServiceClient, typeManager, metricsCollectionService,
                                 opExecutor, new HashSet<DatasetMetricsReporter>(), typeService, instanceService);

    // Start dataset service, wait for it to be discoverable
    service.startAndWait();
    waitForService(Constants.Service.DATASET_EXECUTOR);
    waitForService(Constants.Service.DATASET_MANAGER);
    // this usually happens while creating a namespace, however not doing that in data fabric tests
    Locations.mkdirsIfNotExists(namespacedLocationFactory.get(Id.Namespace.DEFAULT));
  }

  private static void waitForService(String service) {
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(discoveryServiceClient.discover(service));
    Preconditions.checkNotNull(endpointStrategy.pick(5, TimeUnit.SECONDS),
                               "%s service is not up after 5 seconds", service);
  }

  private synchronized int getPort() {
    int attempts = 0;
    while (port < 0 && attempts++ < 10) {
      ServiceDiscovered discovered = discoveryServiceClient.discover(Constants.Service.DATASET_MANAGER);
      if (!discovered.iterator().hasNext()) {
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        continue;
      }
      port = discovered.iterator().next().getSocketAddress().getPort();
    }

    return port;
  }

  protected static CConfiguration createCConf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    File dataDir = new File(TMP_FOLDER.newFolder(), "data");
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, dataDir.getAbsolutePath());
    if (!DirUtils.mkdirs(dataDir)) {
      throw new RuntimeException(String.format("Could not create DatasetFramework output dir %s", dataDir));
    }
    cConf.set(Constants.Dataset.Manager.OUTPUT_DIR, dataDir.getAbsolutePath());
    cConf.set(Constants.Dataset.Manager.ADDRESS, "localhost");
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    return cConf;
  }

  protected URL getUrl(String path) throws MalformedURLException {
    return getUrl(NamespaceId.DEFAULT.getNamespace(), path);
  }

  protected URL getUrl(String namespace, String path) throws MalformedURLException {
    return new URL(
      URI.create(String.format("http://localhost:%d/%s/namespaces/%s%s",
                               getPort(), Constants.Gateway.API_VERSION_3_TOKEN, namespace, path)).toASCIIString());
  }

  protected Location createModuleJar(Class moduleClass, Location...bundleEmbeddedJars) throws IOException {
    LocationFactory lf = new LocalLocationFactory(TMP_FOLDER.newFolder());
    File[] embeddedJars = new File[bundleEmbeddedJars.length];
    for (int i = 0; i < bundleEmbeddedJars.length; i++) {
      File file = TMP_FOLDER.newFile();
      Files.copy(Locations.newInputSupplier(bundleEmbeddedJars[i]), file);
      embeddedJars[i] = file;
    }

    return AppJarHelper.createDeploymentJar(lf, moduleClass, embeddedJars);
  }

  protected HttpResponse deployModule(String moduleName, Class moduleClass) throws Exception {
    return deployModule(Id.DatasetModule.from(Id.Namespace.DEFAULT, moduleName), moduleClass);
  }

  protected HttpResponse deployModule(Id.DatasetModule module, Class moduleClass) throws Exception {
    return deployModule(module, moduleClass, false);
  }

  protected HttpResponse deployModule(String moduleName, Class moduleClass, boolean force) throws Exception {
    return deployModule(Id.DatasetModule.from(Id.Namespace.DEFAULT, moduleName), moduleClass, force);
  }

  protected HttpResponse deployModule(Id.DatasetModule module, Class moduleClass, boolean force) throws Exception {
    Location moduleJar = createModuleJar(moduleClass);
    String urlPath = "/data/modules/" + module.getId();
    urlPath = force ? urlPath + "?force=true" : urlPath;
    HttpRequest request = HttpRequest.put(getUrl(module.getNamespaceId(), urlPath))
      .addHeader("X-Class-Name", moduleClass.getName())
      .withBody(Locations.newInputSupplier(moduleJar)).build();
    return HttpRequests.execute(request);
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
    return getModules(Id.Namespace.DEFAULT);
  }

  protected ObjectResponse<List<DatasetModuleMeta>> getModules(Id.Namespace namespace) throws IOException {
    return ObjectResponse.fromJsonBody(makeModulesRequest(namespace),
                                       new TypeToken<List<DatasetModuleMeta>>() { }.getType());
  }

  protected HttpResponse makeModulesRequest(Id.Namespace namespaceId) throws IOException {
    HttpRequest request = HttpRequest.get(getUrl(namespaceId.getId(), "/data/modules")).build();
    return HttpRequests.execute(request);
  }

  protected HttpResponse deleteModule(String moduleName) throws Exception {
    return deleteModule(Id.DatasetModule.from(Id.Namespace.DEFAULT, moduleName));
  }

  protected HttpResponse deleteModule(Id.DatasetModule module) throws Exception {
    return HttpRequests.execute(
      HttpRequest.delete(getUrl(module.getNamespaceId(), "/data/modules/" + module.getId())).build());
  }

  protected HttpResponse deleteModules() throws IOException {
    return deleteModules(Id.Namespace.DEFAULT);
  }

  protected HttpResponse deleteModules(Id.Namespace namespace) throws IOException {
    return HttpRequests.execute(HttpRequest.delete(getUrl(namespace.getId(), "/data/modules/")).build());
  }

  protected void assertNamespaceNotFound(HttpResponse response, Id.Namespace namespaceId) {
    Assert.assertEquals(HttpStatus.SC_NOT_FOUND, response.getResponseCode());
    Assert.assertTrue(response.getResponseBodyAsString().contains(namespaceId.toString()));
  }
}
