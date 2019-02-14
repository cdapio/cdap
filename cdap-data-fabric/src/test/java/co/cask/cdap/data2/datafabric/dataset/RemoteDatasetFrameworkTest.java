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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfigurationUtil;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.InMemoryDiscoveryModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data.runtime.StorageModule;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import co.cask.cdap.data2.datafabric.dataset.service.AuthorizationDatasetTypeService;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetInstanceService;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetTypeService;
import co.cask.cdap.data2.datafabric.dataset.service.DefaultDatasetTypeService;
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
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.metadata.writer.NoOpMetadataPublisher;
import co.cask.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import co.cask.cdap.data2.transaction.TransactionSystemClientService;
import co.cask.cdap.explore.client.DiscoveryExploreClient;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.DefaultImpersonator;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.store.StoreDefinition;
import co.cask.http.HttpHandler;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.tephra.runtime.TransactionInMemoryModule;
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
  private TransactionRunner transactionRunner;

  @Before
  public void before() throws Exception {
    cConf.set(Constants.Service.MASTER_SERVICES_BIND_ADDRESS, "localhost");
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);

    Configuration txConf = HBaseConfiguration.create();
    CConfigurationUtil.copyTxProperties(cConf, txConf);

    // ok to pass null, since the impersonator won't actually be called, if kerberos security is not enabled
    Impersonator impersonator = new DefaultImpersonator(cConf, null);

    // TODO: Refactor to use injector for everything
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, txConf),
      new InMemoryDiscoveryModule(),
      new AuthorizationTestModule(),
      new StorageModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new TransactionInMemoryModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Singleton.class);
          bind(DatasetDefinitionRegistryFactory.class)
            .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);
          // through the injector, we only need RemoteDatasetFramework in these tests
          bind(RemoteDatasetFramework.class);
        }
      }
    );

    // Tx Manager to support working with datasets
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    transactionRunner = injector.getInstance(TransactionRunner.class);
    StructuredTableAdmin structuredTableAdmin = injector.getInstance(StructuredTableAdmin.class);
    StructuredTableRegistry structuredTableRegistry = injector.getInstance(StructuredTableRegistry.class);
    StoreDefinition.createAllTables(structuredTableAdmin, structuredTableRegistry);
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
      new DatasetAdminService(framework, cConf, locationFactory, datasetInstantiatorFactory, impersonator);
    ImmutableSet<HttpHandler> handlers =
      ImmutableSet.of(new DatasetAdminOpHTTPHandler(datasetAdminService));
    opExecutorService = new DatasetOpExecutorService(cConf, discoveryService, metricsCollectionService, handlers);
    opExecutorService.startAndWait();

    DiscoveryExploreClient exploreClient = new DiscoveryExploreClient(discoveryServiceClient, authenticationContext);
    ExploreFacade exploreFacade = new ExploreFacade(exploreClient, cConf);
    AuthorizationEnforcer authorizationEnforcer = injector.getInstance(AuthorizationEnforcer.class);

    DatasetTypeManager typeManager = new DatasetTypeManager(cConf, locationFactory, impersonator, transactionRunner);
    DatasetInstanceManager instanceManager = new DatasetInstanceManager(transactionRunner);
    DatasetTypeService noAuthTypeService = new DefaultDatasetTypeService(typeManager, namespaceQueryAdmin,
                                                                         namespacePathLocator, cConf, impersonator,
                                                                         txSystemClientService, transactionRunner,
                                                                         DEFAULT_MODULES);
    DatasetTypeService typeService = new AuthorizationDatasetTypeService(noAuthTypeService, authorizationEnforcer,
                                                                         authenticationContext);


    DatasetOpExecutor opExecutor = new LocalDatasetOpExecutor(cConf, discoveryServiceClient, opExecutorService,
                                                              authenticationContext);
    DatasetInstanceService instanceService = new DatasetInstanceService(cConf,
                                                                        typeService, noAuthTypeService,
                                                                        instanceManager, opExecutor,
                                                                        exploreFacade, namespaceQueryAdmin, ownerAdmin,
                                                                        authorizationEnforcer, authenticationContext,
                                                                        new NoOpMetadataPublisher());
    instanceService.setAuditPublisher(inMemoryAuditPublisher);

    service = new DatasetService(cConf, discoveryService, discoveryServiceClient, metricsCollectionService,
                                 new InMemoryDatasetOpExecutor(framework), new HashSet<>(),
                                 typeService, instanceService);
    // Start dataset service, wait for it to be discoverable
    service.startAndWait();
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(
      () -> discoveryServiceClient.discover(Constants.Service.DATASET_MANAGER));
    Preconditions.checkNotNull(endpointStrategy.pick(5, TimeUnit.SECONDS),
                               "%s service is not up after 5 seconds", service);
  }

  // Note: Cannot have these system namespace restrictions in system namespace since we use it internally in
  // DatasetMetaTable util to add modules to system namespace. However, we should definitely impose these restrictions
  // in RemoteDatasetFramework.
  @Test
  public void testSystemNamespace() {
    DatasetFramework framework = getFramework();
    try {
      framework.deleteModule(NamespaceId.SYSTEM.datasetModule("orderedTable-memory"));
      Assert.fail("Should not be able to delete a default module.");
    } catch (DatasetManagementException e) {
      // expected
    }
    try {
      framework.deleteAllModules(NamespaceId.SYSTEM);
      Assert.fail("Should not be able to delete modules from system namespace");
    } catch (DatasetManagementException e) {
      // expected
    }
  }

  @After
  public void after() {
    Futures.getUnchecked(Services.chainStop(service, opExecutorService, txManager));
  }

  @Override
  protected DatasetFramework getFramework() {
    return framework;
  }
}
