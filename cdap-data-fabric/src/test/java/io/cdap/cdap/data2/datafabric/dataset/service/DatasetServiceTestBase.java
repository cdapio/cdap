/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.service;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.reflect.TypeToken;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data2.datafabric.dataset.RemoteDatasetFramework;
import io.cdap.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpHTTPHandler;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetAdminService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.InMemoryDatasetOpExecutor;
import io.cdap.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import io.cdap.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.InMemoryDatasetFramework;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
import io.cdap.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import io.cdap.cdap.data2.transaction.TransactionSystemClientService;
import io.cdap.cdap.explore.client.DiscoveryExploreClient;
import io.cdap.cdap.explore.client.ExploreFacade;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.URIScheme;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.DefaultImpersonator;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.impersonation.InMemoryOwnerStore;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerStore;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.common.ContentProvider;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;
import io.cdap.http.HttpHandler;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.runtime.TransactionInMemoryModule;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
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
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Base class for unit-tests that require running of {@link DatasetService}
 */
public abstract class DatasetServiceTestBase {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  protected static final HttpRequestConfig REQUEST_CONFIG = new DefaultHttpRequestConfig(false);

  protected static LocationFactory locationFactory;
  protected static NamespaceAdmin namespaceAdmin;
  protected static TransactionManager txManager;
  protected static RemoteDatasetFramework dsFramework;
  protected static InMemoryDatasetFramework inMemoryDatasetFramework;
  protected static DatasetInstanceService instanceService;
  protected static DatasetDefinitionRegistryFactory registryFactory;
  protected static Injector injector;
  protected static TransactionRunner transactionRunner;

  private static DiscoveryServiceClient discoveryServiceClient;
  private static DatasetOpExecutorService opExecutorService;
  private static DatasetService service;
  protected static OwnerAdmin ownerAdmin;

  private final int port = -1;

  protected static void initialize() throws Exception {
    locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    initializeAndStartService(createCConf());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Services.chainStop(service, opExecutorService, txManager);
    namespaceAdmin.delete(NamespaceId.DEFAULT);
    Locations.deleteQuietly(locationFactory.create(NamespaceId.DEFAULT.getNamespace()));
  }

  protected static void initializeAndStartService(CConfiguration cConf) throws Exception {
    // TODO: this whole method is a mess. Streamline it!
    injector = Guice.createInjector(
      new ConfigModule(cConf),
      new InMemoryDiscoveryModule(),
      new NonCustomLocationUnitTestModule(),
      new NamespaceAdminTestModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new TransactionInMemoryModule(),
      new AuthorizationTestModule(),
      new StorageModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Singleton.class);
          bind(DatasetDefinitionRegistryFactory.class)
            .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);
          // through the injector, we only need RemoteDatasetFramework in these tests
          bind(RemoteDatasetFramework.class);
          bind(OwnerStore.class).to(InMemoryOwnerStore.class);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
        }
      });

    AuthorizationEnforcer authEnforcer = injector.getInstance(AuthorizationEnforcer.class);

    AuthenticationContext authenticationContext = injector.getInstance(AuthenticationContext.class);

    transactionRunner = injector.getInstance(TransactionRunner.class);

    DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);
    discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    dsFramework = injector.getInstance(RemoteDatasetFramework.class);
    // Tx Manager to support working with datasets
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    StructuredTableAdmin structuredTableAdmin = injector.getInstance(StructuredTableAdmin.class);
    StructuredTableRegistry structuredTableRegistry = injector.getInstance(StructuredTableRegistry.class);
    StoreDefinition.createAllTables(structuredTableAdmin, structuredTableRegistry);
    TransactionSystemClient txSystemClient = injector.getInstance(TransactionSystemClient.class);
    TransactionSystemClientService txSystemClientService =
      new DelegatingTransactionSystemClientService(txSystemClient);

    NamespacePathLocator namespacePathLocator = injector.getInstance(NamespacePathLocator.class);
    SystemDatasetInstantiatorFactory datasetInstantiatorFactory =
      new SystemDatasetInstantiatorFactory(locationFactory, dsFramework, cConf);

    // ok to pass null, since the impersonator won't actually be called, if kerberos security is not enabled
    Impersonator impersonator = new DefaultImpersonator(cConf, null);
    DatasetAdminService datasetAdminService =
      new DatasetAdminService(dsFramework, cConf, locationFactory, datasetInstantiatorFactory, impersonator);
    ImmutableSet<HttpHandler> handlers =
      ImmutableSet.of(new DatasetAdminOpHTTPHandler(datasetAdminService));
    MetricsCollectionService metricsCollectionService = injector.getInstance(MetricsCollectionService.class);

    opExecutorService = new DatasetOpExecutorService(cConf, SConfiguration.create(),
                                                     discoveryService, metricsCollectionService, handlers);
    opExecutorService.startAndWait();

    Map<String, DatasetModule> defaultModules =
      injector.getInstance(Key.get(new TypeLiteral<Map<String, DatasetModule>>() {
                                   },
                                   Constants.Dataset.Manager.DefaultDatasetModules.class));

    ImmutableMap<String, DatasetModule> modules = ImmutableMap.<String, DatasetModule>builder()
      .putAll(defaultModules)
      .build();

    registryFactory = injector.getInstance(DatasetDefinitionRegistryFactory.class);
    inMemoryDatasetFramework = new InMemoryDatasetFramework(registryFactory, modules);

    DiscoveryExploreClient exploreClient = new DiscoveryExploreClient(discoveryServiceClient, authenticationContext);
    ExploreFacade exploreFacade = new ExploreFacade(exploreClient, cConf);
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespaceAdmin.create(NamespaceMeta.DEFAULT);
    ownerAdmin = injector.getInstance(OwnerAdmin.class);
    NamespaceQueryAdmin namespaceQueryAdmin = injector.getInstance(NamespaceQueryAdmin.class);
    DatasetTypeManager typeManager = new DatasetTypeManager(cConf, locationFactory, impersonator, transactionRunner);
    DatasetOpExecutor opExecutor = new InMemoryDatasetOpExecutor(dsFramework);
    DatasetInstanceManager instanceManager =
      new DatasetInstanceManager(transactionRunner);

    DatasetTypeService noAuthTypeService = new DefaultDatasetTypeService(
      typeManager, namespaceAdmin, namespacePathLocator,
      cConf, impersonator, txSystemClientService, transactionRunner, defaultModules);
    DatasetTypeService typeService = new AuthorizationDatasetTypeService(noAuthTypeService, authEnforcer,
                                                                         authenticationContext);

    instanceService = new DatasetInstanceService(typeService, noAuthTypeService,
                                                 instanceManager, opExecutor, exploreFacade,
                                                 namespaceQueryAdmin, ownerAdmin, authEnforcer,
                                                 authenticationContext,
                                                 new NoOpMetadataServiceClient());

    service = new DatasetService(cConf, SConfiguration.create(),
                                 discoveryService, discoveryServiceClient, metricsCollectionService,
                                 new HashSet<>(), typeService, instanceService);

    // Start dataset service, wait for it to be discoverable
    service.startAndWait();
    waitForService(Constants.Service.DATASET_EXECUTOR);
    waitForService(Constants.Service.DATASET_MANAGER);
    // this usually happens while creating a namespace, however not doing that in data fabric tests
    Locations.mkdirsIfNotExists(namespacePathLocator.get(NamespaceId.DEFAULT));
  }

  private static void waitForService(String service) {
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(() -> discoveryServiceClient.discover(service));
    Preconditions.checkNotNull(endpointStrategy.pick(5, TimeUnit.SECONDS),
                               "%s service is not up after 5 seconds", service);
  }

  @Nullable
  private synchronized Discoverable getDiscoverable() {
    Discoverable discoverable = new RandomEndpointStrategy(
      () -> discoveryServiceClient.discover(Constants.Service.DATASET_MANAGER))
      .pick(10, TimeUnit.SECONDS);

    Preconditions.checkNotNull(discoverable, "No endpoint discovered for service %s",
                               Constants.Service.DATASET_MANAGER);
    return discoverable;
  }

  protected static CConfiguration createCConf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    File dataDir = new File(TMP_FOLDER.newFolder(), "data");
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, dataDir.getAbsolutePath());
    if (!DirUtils.mkdirs(dataDir)) {
      throw new RuntimeException(String.format("Could not create DatasetFramework output dir %s", dataDir));
    }
    cConf.set(Constants.Dataset.Manager.OUTPUT_DIR, dataDir.getAbsolutePath());
    cConf.set(Constants.Service.MASTER_SERVICES_BIND_ADDRESS, "localhost");
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    return cConf;
  }

  protected URL getUrl(String path) throws MalformedURLException {
    return getUrl(NamespaceId.DEFAULT.getNamespace(), path);
  }

  protected URL getUrl(String namespace, String path) throws MalformedURLException {
    return URIScheme.createURI(getDiscoverable(), "%s/namespaces/%s%s",
                               Constants.Gateway.API_VERSION_3_TOKEN, namespace, path).toURL();
  }

  protected Location createModuleJar(Class moduleClass, Location... bundleEmbeddedJars) throws IOException {
    LocationFactory lf = new LocalLocationFactory(TMP_FOLDER.newFolder());
    File[] embeddedJars = new File[bundleEmbeddedJars.length];
    for (int i = 0; i < bundleEmbeddedJars.length; i++) {
      File file = TMP_FOLDER.newFile();
      Locations.linkOrCopyOverwrite(bundleEmbeddedJars[i], file);
      embeddedJars[i] = file;
    }

    return AppJarHelper.createDeploymentJar(lf, moduleClass, embeddedJars);
  }

  protected HttpResponse deployModule(String moduleName, Class moduleClass) throws Exception {
    return deployModule(NamespaceId.DEFAULT.datasetModule(moduleName), moduleClass);
  }

  protected HttpResponse deployModule(DatasetModuleId module, Class moduleClass) throws Exception {
    return deployModule(module, moduleClass, false);
  }

  protected HttpResponse deployModule(String moduleName, Class moduleClass, boolean force) throws Exception {
    return deployModule(NamespaceId.DEFAULT.datasetModule(moduleName), moduleClass, force);
  }

  protected HttpResponse deployModule(DatasetModuleId module, Class moduleClass, boolean force) throws Exception {
    Location moduleJar = createModuleJar(moduleClass);
    String urlPath = "/data/modules/" + module.getEntityName();
    urlPath = force ? urlPath + "?force=true" : urlPath;
    HttpRequest request = HttpRequest.put(getUrl(module.getNamespace(), urlPath))
      .addHeader("X-Class-Name", moduleClass.getName())
      .withBody((ContentProvider<? extends InputStream>) moduleJar::getInputStream).build();
    return HttpRequests.execute(request, REQUEST_CONFIG);
  }

  // creates a bundled jar with moduleClass and list of bundleEmbeddedJar files, moduleName and moduleClassName are
  // used to make request for deploying module.
  protected int deployModuleBundled(String moduleName, String moduleClassName, Class moduleClass,
                                    Location... bundleEmbeddedJars) throws IOException {
    Location moduleJar = createModuleJar(moduleClass, bundleEmbeddedJars);
    HttpRequest request = HttpRequest.put(getUrl("/data/modules/" + moduleName))
      .addHeader("X-Class-Name", moduleClassName)
      .withBody((ContentProvider<? extends InputStream>) moduleJar::getInputStream).build();
    return HttpRequests.execute(request, REQUEST_CONFIG).getResponseCode();
  }

  protected ObjectResponse<List<DatasetModuleMeta>> getModules() throws IOException {
    return getModules(NamespaceId.DEFAULT);
  }

  protected ObjectResponse<List<DatasetModuleMeta>> getModules(NamespaceId namespace) throws IOException {
    return ObjectResponse.fromJsonBody(makeModulesRequest(namespace),
                                       new TypeToken<List<DatasetModuleMeta>>() {
                                       }.getType());
  }

  protected HttpResponse makeModulesRequest(NamespaceId namespaceId) throws IOException {
    HttpRequest request = HttpRequest.get(getUrl(namespaceId.getEntityName(), "/data/modules")).build();
    return HttpRequests.execute(request, REQUEST_CONFIG);
  }

  protected HttpResponse deleteModule(String moduleName) throws Exception {
    return deleteModule(NamespaceId.DEFAULT.datasetModule(moduleName));
  }

  protected HttpResponse deleteModule(DatasetModuleId module) throws Exception {
    return HttpRequests.execute(
      HttpRequest.delete(getUrl(module.getNamespace(), "/data/modules/" + module.getEntityName())).build(),
      REQUEST_CONFIG);
  }

  protected HttpResponse deleteModules() throws IOException {
    return deleteModules(NamespaceId.DEFAULT);
  }

  protected HttpResponse deleteModules(NamespaceId namespace) throws IOException {
    return HttpRequests.execute(HttpRequest.delete(getUrl(namespace.getEntityName(), "/data/modules/")).build(),
                                REQUEST_CONFIG);
  }

  protected void assertNamespaceNotFound(HttpResponse response, NamespaceId namespaceId) {
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, response.getResponseCode());
    Assert.assertTrue(response.getResponseBodyAsString().contains(namespaceId.toString()));
  }
}
