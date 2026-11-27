/*
 * Copyright © 2014-2022 Cask Data, Inc.
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfigurationUtil;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
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
import io.cdap.cdap.data2.datafabric.dataset.service.mds.DatasetTypeTable;
import io.cdap.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import io.cdap.cdap.data2.dataset2.AbstractDatasetFrameworkTest;
import io.cdap.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.SimpleKVTable;
import io.cdap.cdap.data2.dataset2.SingleTypeModule;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
import io.cdap.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import io.cdap.cdap.data2.transaction.TransactionSystemClientService;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.DefaultImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.http.HttpHandler;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
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

    Configuration txConf = new Configuration();
    CConfigurationUtil.copyTxProperties(cConf, txConf);

    // ok to pass null, since the impersonator won't actually be called, if kerberos security is not enabled
    Impersonator impersonator = new DefaultImpersonator(cConf, null);

    // TODO: Refactor to use injector for everything
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, txConf),
      RemoteAuthenticatorModules.getNoOpModule(),
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
    txManager.startAsync().awaitRunning();
    transactionRunner = injector.getInstance(TransactionRunner.class);
    StructuredTableAdmin structuredTableAdmin = injector.getInstance(StructuredTableAdmin.class);
    StoreDefinition.createAllTables(structuredTableAdmin);
    InMemoryTxSystemClient txSystemClient = new InMemoryTxSystemClient(txManager);
    TransactionSystemClientService txSystemClientService = new DelegatingTransactionSystemClientService(txSystemClient);

    DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    CommonNettyHttpServiceFactory commonNettyHttpServiceFactory =
      injector.getInstance(CommonNettyHttpServiceFactory.class);
    AuthenticationContext authenticationContext = injector.getInstance(AuthenticationContext.class);
    RemoteClientFactory remoteClientFactory = injector.getInstance(RemoteClientFactory.class);

    framework = createFramework(authenticationContext, remoteClientFactory);
    SystemDatasetInstantiatorFactory datasetInstantiatorFactory =
      new SystemDatasetInstantiatorFactory(framework);

    // dataset modules cannot be added through the remote framework, hence the modules used by the tests
    // are registered as system modules
    Map<String, DatasetModule> systemModules = new LinkedHashMap<>(DEFAULT_MODULES);
    systemModules.putAll(TEST_MODULES);

    DatasetAdminService datasetAdminService =
      new DatasetAdminService(framework, cConf, datasetInstantiatorFactory, impersonator, systemModules);
    ImmutableSet<HttpHandler> handlers =
      ImmutableSet.of(new DatasetAdminOpHTTPHandler(datasetAdminService));
    opExecutorService = new DatasetOpExecutorService(cConf, SConfiguration.create(),
                                                     discoveryService, commonNettyHttpServiceFactory, handlers);
    opExecutorService.startAsync().awaitRunning();

    AccessEnforcer accessEnforcer = injector.getInstance(AccessEnforcer.class);

    DatasetTypeManager typeManager = new DatasetTypeManager(locationFactory, impersonator, transactionRunner);
    DatasetInstanceManager instanceManager = new DatasetInstanceManager(transactionRunner);
    DatasetTypeService noAuthTypeService = new DefaultDatasetTypeService(typeManager, namespaceQueryAdmin, cConf,
                                                                         txSystemClientService, transactionRunner,
                                                                         systemModules);
    DatasetTypeService typeService = new AuthorizationDatasetTypeService(noAuthTypeService, accessEnforcer,
                                                                         authenticationContext);


    DatasetOpExecutor opExecutor = new RemoteDatasetOpExecutor(authenticationContext, remoteClientFactory);
    DatasetInstanceService instanceService = new DatasetInstanceService(typeService, noAuthTypeService,
                                                                        instanceManager, opExecutor,
                                                                        namespaceQueryAdmin, ownerAdmin,
                                                                        accessEnforcer, authenticationContext,
                                                                        new NoOpMetadataServiceClient());
    instanceService.setAuditPublisher(inMemoryAuditPublisher);

    service = new DatasetService(cConf, SConfiguration.create(),
                                 discoveryService, discoveryServiceClient, commonNettyHttpServiceFactory,
                                 new HashSet<>(),
                                 typeService, instanceService);
    // Start dataset service, wait for it to be discoverable
    service.startAsync().awaitRunning();
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(
      () -> discoveryServiceClient.discover(Constants.Service.DATASET_MANAGER));
    Preconditions.checkNotNull(endpointStrategy.pick(5, TimeUnit.SECONDS),
                               "%s service is not up after 5 seconds", service);
  }

  /**
   * This method allows to replace / mock remote clients
   */
  protected RemoteDatasetFramework createFramework(AuthenticationContext authenticationContext,
                                                   RemoteClientFactory remoteClientFactory) {
    return new RemoteDatasetFramework(cConf, registryFactory, authenticationContext, remoteClientFactory);
  }

  // Note: Cannot have these system namespace restrictions in system namespace since we use it internally in
  // DatasetMetaTable util to add modules to system namespace. However, we should definitely impose these restrictions
  // in RemoteDatasetFramework.
  @Test
  public void testSystemNamespace() throws UnauthorizedException {
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

  @Test
  public void testAddModuleNotSupported() throws Exception {
    DatasetFramework framework = getFramework();
    DatasetModuleId moduleId = NAMESPACE_ID.datasetModule("keyValue");
    try {
      framework.addModule(moduleId, new SingleTypeModule(SimpleKVTable.class));
      Assert.fail("Should not be able to add a dataset module");
    } catch (UnsupportedOperationException e) {
      // expected
    }
    try {
      framework.addModule(moduleId, new SingleTypeModule(SimpleKVTable.class),
                          locationFactory.create("keyValue.jar"));
      Assert.fail("Should not be able to add a dataset module");
    } catch (UnsupportedOperationException e) {
      // expected
    }
    Assert.assertFalse(framework.hasType(NAMESPACE_ID.datasetType(SimpleKVTable.class.getName())));
  }

  /**
   * Tests that a dataset cannot be instantiated if its type is defined by a module that has a jar location.
   */
  @Test
  public void testCustomTypeNotSupported() throws Exception {
    DatasetFramework framework = getFramework();
    DatasetId datasetId = NAMESPACE_ID.dataset("customDataset");
    // write a module with a jar location and an instance of its type to the store. The module class is not
    // available in the classpath.
    DatasetModuleMeta moduleMeta = new DatasetModuleMeta(
      "customModule", "io.cdap.cdap.test.NotInClasspathModule",
      locationFactory.create("customModule.jar").toURI(),
      ImmutableList.of("customType"), Collections.emptyList());
    TransactionRunners.run(transactionRunner, context -> {
      DatasetTypeTable.create(context).writeModule(NAMESPACE_ID, moduleMeta);
    });
    new DatasetInstanceManager(transactionRunner)
      .add(NAMESPACE_ID, DatasetSpecification.builder(datasetId.getDataset(), "customType").build());

    // the dataset exists, but neither the dataset nor its admin can be instantiated
    Assert.assertTrue(framework.hasInstance(datasetId));
    Assert.assertNotNull(framework.getDatasetSpec(datasetId));
    try {
      framework.getDataset(datasetId, DatasetDefinition.NO_ARGUMENTS, null);
      Assert.fail("Should not be able to instantiate a dataset of a custom type");
    } catch (DatasetManagementException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("custom dataset types are not supported"));
    }
    try {
      framework.getAdmin(datasetId, null);
      Assert.fail("Should not be able to instantiate the admin of a dataset of a custom type");
    } catch (DatasetManagementException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("custom dataset types are not supported"));
    }

    // the dataset cannot be updated or truncated, and no other dataset of that type can be created
    try {
      framework.updateInstance(datasetId, DatasetProperties.EMPTY);
      Assert.fail("Should not be able to update a dataset of a custom type");
    } catch (DatasetManagementException e) {
      // expected
    }
    try {
      framework.truncateInstance(datasetId);
      Assert.fail("Should not be able to truncate a dataset of a custom type");
    } catch (DatasetManagementException e) {
      // expected
    }
    try {
      framework.addInstance("customType", NAMESPACE_ID.dataset("customDataset2"), DatasetProperties.EMPTY);
      Assert.fail("Should not be able to create a dataset of a custom type");
    } catch (DatasetManagementException e) {
      // expected
    }
    Assert.assertFalse(framework.hasInstance(NAMESPACE_ID.dataset("customDataset2")));

    // the dataset and the module can be deleted
    framework.deleteInstance(datasetId);
    Assert.assertFalse(framework.hasInstance(datasetId));
    framework.deleteAllModules(NAMESPACE_ID);
    Assert.assertFalse(framework.hasType(NAMESPACE_ID.datasetType("customType")));
  }

  @After
  public void after() {
    Futures.getUnchecked(Services.chainStop(service, opExecutorService, txManager));
  }

  @Override
  protected DatasetFramework getFramework() {
    return framework;
  }

  @Override
  protected boolean supportsAddModule() {
    return false;
  }
}
