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
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfigurationUtil;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data.runtime.DynamicTransactionExecutorFactory;
import co.cask.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetInstanceService;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetTypeService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpHTTPHandler;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.InMemoryDatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.service.executor.LocalDatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.data2.dataset2.AbstractDatasetFrameworkTest;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.SimpleKVTable;
import co.cask.cdap.data2.dataset2.SingleTypeModule;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import co.cask.cdap.data2.metadata.store.NoOpMetadataStore;
import co.cask.cdap.data2.metrics.DatasetMetricsReporter;
import co.cask.cdap.data2.security.Impersonator;
import co.cask.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.data2.transaction.TransactionSystemClientService;
import co.cask.cdap.explore.client.DiscoveryExploreClient;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.http.HttpHandler;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import co.cask.tephra.runtime.TransactionInMemoryModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.Services;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link RemoteDatasetFramework}
 */
public class RemoteDatasetFrameworkTest extends AbstractDatasetFrameworkTest {
  private TransactionManager txManager;
  private DatasetOpExecutorService opExecutorService;
  private DatasetService service;
  private RemoteDatasetFramework framework;

  @Before
  public void before() throws Exception {
    cConf.set(Constants.Dataset.Manager.ADDRESS, "localhost");
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);

    Configuration txConf = HBaseConfiguration.create();
    CConfigurationUtil.copyTxProperties(cConf, txConf);

    // ok to pass null, since the impersonator won't actually be called, if kerberos security is not enabled
    Impersonator impersonator = new Impersonator(cConf, null, null);

    // TODO: Refactor to use injector for everything
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, txConf),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new TransactionInMemoryModule(),
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
      }
    );

    // Tx Manager to support working with datasets
    txManager = new TransactionManager(txConf);
    txManager.startAndWait();
    InMemoryTxSystemClient txSystemClient = new InMemoryTxSystemClient(txManager);
    TransactionSystemClientService txSystemClientService = new DelegatingTransactionSystemClientService(txSystemClient);

    DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    MetricsCollectionService metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    AuthenticationContext authenticationContext = injector.getInstance(AuthenticationContext.class);

    framework = new RemoteDatasetFramework(cConf, discoveryServiceClient, registryFactory, authenticationContext);
    SystemDatasetInstantiatorFactory datasetInstantiatorFactory =
      new SystemDatasetInstantiatorFactory(locationFactory, framework, cConf);

    DatasetAdminService datasetAdminService =
      new DatasetAdminService(framework, cConf, locationFactory, datasetInstantiatorFactory, new NoOpMetadataStore(),
                              impersonator);
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

    ExploreFacade exploreFacade = new ExploreFacade(new DiscoveryExploreClient(cConf, discoveryServiceClient), cConf);
    TransactionExecutorFactory txExecutorFactory = new DynamicTransactionExecutorFactory(txSystemClient);
    AuthorizationEnforcer authorizationEnforcer = injector.getInstance(AuthorizationEnforcer.class);

    DatasetTypeManager typeManager = new DatasetTypeManager(cConf, locationFactory, txSystemClientService,
                                                            txExecutorFactory, mdsFramework, DEFAULT_MODULES,
                                                            impersonator);
    DatasetInstanceManager instanceManager = new DatasetInstanceManager(txSystemClientService, txExecutorFactory,
                                                                        mdsFramework);
    AuthorizerInstantiator authorizerInstantiator = injector.getInstance(AuthorizerInstantiator.class);
    DatasetTypeService typeService = new DatasetTypeService(typeManager, namespaceQueryAdmin, namespacedLocationFactory,
                                                            authorizationEnforcer, authorizerInstantiator,
                                                            authenticationContext, cConf, impersonator);
    DatasetOpExecutor opExecutor = new LocalDatasetOpExecutor(cConf, discoveryServiceClient, opExecutorService);
    DatasetInstanceService instanceService = new DatasetInstanceService(
      typeManager, instanceManager, opExecutor, exploreFacade, namespaceQueryAdmin, authorizationEnforcer,
      authorizerInstantiator, authenticationContext);
    instanceService.setAuditPublisher(inMemoryAuditPublisher);

    service = new DatasetService(cConf, discoveryService, discoveryServiceClient, typeManager, metricsCollectionService,
                                 new InMemoryDatasetOpExecutor(framework), new HashSet<DatasetMetricsReporter>(),
                                 typeService, instanceService);
    // Start dataset service, wait for it to be discoverable
    service.startAndWait();
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(discoveryServiceClient.discover(
      Constants.Service.DATASET_MANAGER)
    );
    Preconditions.checkNotNull(endpointStrategy.pick(5, TimeUnit.SECONDS),
                               "%s service is not up after 5 seconds", service);

    createNamespace(Id.Namespace.SYSTEM);
    createNamespace(NAMESPACE_ID);
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

  private void createNamespace (Id.Namespace namespaceId) throws Exception {
    // since the namespace admin here is an in memory one we need to create the location explicitly
    namespacedLocationFactory.get(namespaceId).mkdirs();
    // the framework.delete looks up namespace config through namespaceadmin add the meta there too.
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(namespaceId).build());
  }

  private void deleteNamespace (Id.Namespace namespaceId) throws Exception {
    // since the namespace admin here is an in memory one we need to delete the location explicitly
    namespacedLocationFactory.get(namespaceId).delete(true);
    namespaceAdmin.delete(namespaceId);
  }

  @After
  public void after() throws Exception {
    // since we stored namespace meta through admin so that framework.delete can lookup namespaceconfig clean the
    // meta from there too
    deleteNamespace(NAMESPACE_ID);
    deleteNamespace(Id.Namespace.SYSTEM);
    Futures.getUnchecked(Services.chainStop(service, opExecutorService, txManager));
  }

  @Override
  protected DatasetFramework getFramework() {
    return framework;
  }
}
