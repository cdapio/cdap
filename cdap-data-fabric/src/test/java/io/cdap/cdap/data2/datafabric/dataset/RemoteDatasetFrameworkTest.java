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

package io.cdap.cdap.data2.datafabric.dataset;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfigurationUtil;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import io.cdap.cdap.data2.datafabric.dataset.service.AuthorizationDatasetTypeService;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetInstanceService;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetTypeService;
import io.cdap.cdap.data2.datafabric.dataset.service.DefaultDatasetTypeService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpHTTPHandler;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetAdminService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.RemoteDatasetOpExecutor;
import io.cdap.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import io.cdap.cdap.data2.dataset2.AbstractDatasetFrameworkTest;
import io.cdap.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
import io.cdap.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import io.cdap.cdap.data2.transaction.TransactionSystemClientService;
import io.cdap.cdap.explore.client.DiscoveryExploreClient;
import io.cdap.cdap.explore.client.ExploreFacade;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.DefaultImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.http.HttpHandler;
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
    TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
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
    opExecutorService = new DatasetOpExecutorService(cConf, SConfiguration.create(),
                                                     discoveryService, metricsCollectionService, handlers);
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


    DatasetOpExecutor opExecutor = new RemoteDatasetOpExecutor(discoveryServiceClient, authenticationContext);
    DatasetInstanceService instanceService = new DatasetInstanceService(typeService, noAuthTypeService,
                                                                        instanceManager, opExecutor,
                                                                        exploreFacade, namespaceQueryAdmin, ownerAdmin,
                                                                        authorizationEnforcer, authenticationContext,
                                                                        new NoOpMetadataServiceClient());
    instanceService.setAuditPublisher(inMemoryAuditPublisher);

    service = new DatasetService(cConf, SConfiguration.create(),
                                 discoveryService, discoveryServiceClient, metricsCollectionService,
                                 new HashSet<>(),
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
