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

package io.cdap.cdap.explore.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
import io.cdap.cdap.explore.client.DiscoveryExploreClient;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.explore.client.ExploreExecutionResult;
import io.cdap.cdap.explore.executor.ExploreExecutorService;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.explore.guice.ExploreRuntimeModule;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.proto.ColumnDesc;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.QueryHandle;
import io.cdap.cdap.proto.QueryResult;
import io.cdap.cdap.proto.QueryStatus;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.impersonation.UnsupportedUGIProvider;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.tephra.persist.LocalFileTransactionStateStorage;
import org.apache.tephra.persist.TransactionStateStorage;
import org.apache.tephra.runtime.TransactionStateStorageProvider;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Base class for tests that need explore service to be running.
 */
public class BaseHiveExploreServiceTest {

  protected static final NamespaceId NAMESPACE_ID = new NamespaceId("namespace");
  protected static final NamespaceId OTHER_NAMESPACE_ID = new NamespaceId("other");
  protected static final String DEFAULT_DATABASE = NamespaceId.DEFAULT.getNamespace();
  protected static final String NAMESPACE_DATABASE = "cdap_" + NAMESPACE_ID.getNamespace();
  protected static final String OTHER_NAMESPACE_DATABASE = "cdap_" + OTHER_NAMESPACE_ID.getNamespace();
  protected static final DatasetModuleId KEY_STRUCT_VALUE = NAMESPACE_ID.datasetModule("keyStructValue");
  protected static final DatasetId MY_TABLE = NAMESPACE_ID.dataset("my_table");
  protected static final String MY_TABLE_NAME = getDatasetHiveName(MY_TABLE);
  protected static final DatasetModuleId OTHER_KEY_STRUCT_VALUE = OTHER_NAMESPACE_ID.datasetModule("keyStructValue");
  protected static final DatasetId OTHER_MY_TABLE = OTHER_NAMESPACE_ID.dataset("my_table");
  protected static final String OTHER_MY_TABLE_NAME = getDatasetHiveName(OTHER_MY_TABLE);

  // Controls for test suite for whether to run BeforeClass/AfterClass
  // Make sure to reset it back to true after using it in a test class
  public static boolean runBefore = true;
  public static boolean runAfter = true;

  protected static TransactionManager transactionManager;
  protected static TransactionSystemClient transactionSystemClient;
  protected static DatasetFramework datasetFramework;
  protected static DatasetOpExecutorService dsOpService;
  protected static DatasetService datasetService;
  protected static ExploreExecutorService exploreExecutorService;
  protected static ExploreService exploreService;
  protected static DiscoveryExploreClient exploreClient;
  protected static ExploreTableManager exploreTableManager;
  protected static NamespaceAdmin namespaceAdmin;
  private static NamespacePathLocator namespacePathLocator;

  protected static Injector injector;

  protected static void initialize(TemporaryFolder tmpFolder) throws Exception {
    initialize(CConfiguration.create(), tmpFolder);
  }

  protected static void initialize(CConfiguration cConf, TemporaryFolder tmpFolder) throws Exception {
    initialize(cConf, tmpFolder, false);
  }

  protected static void initialize(CConfiguration cConf, TemporaryFolder tmpFolder, boolean enableAuthorization)
    throws Exception {
    if (!runBefore) {
      return;
    }

    Configuration hConf = new Configuration();
    if (enableAuthorization) {
      LocationFactory locationFactory = new LocalLocationFactory(tmpFolder.newFolder());
      Location authExtensionJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAccessController.class);
      cConf.setBoolean(Constants.Security.ENABLED, true);
      cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
      cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, authExtensionJar.toURI().getPath());
      cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
      cConf.setInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES, 0);
    }
    List<Module> modules = createInMemoryModules(cConf, hConf, tmpFolder);
    injector = Guice.createInjector(modules);
    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();
    transactionSystemClient = injector.getInstance(TransactionSystemClient.class);

    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));

    dsOpService = injector.getInstance(DatasetOpExecutorService.class);
    dsOpService.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
    exploreExecutorService.startAndWait();

    datasetFramework = injector.getInstance(DatasetFramework.class);
    exploreClient = injector.getInstance(DiscoveryExploreClient.class);
    exploreService = injector.getInstance(ExploreService.class);
    exploreClient.ping();

    exploreTableManager = injector.getInstance(ExploreTableManager.class);

    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespacePathLocator = injector.getInstance(NamespacePathLocator.class);

    // create namespaces
    // This happens when you create a namespace via REST APIs. However, since we do not start AppFabricServer in
    // Explore tests, simulating that scenario by explicitly calling DatasetFramework APIs.
    createNamespace(NamespaceId.DEFAULT);
    createNamespace(NAMESPACE_ID);
    createNamespace(OTHER_NAMESPACE_ID);
  }

  @AfterClass
  public static void stopServices() throws Exception {
    if (!runAfter) {
      return;
    }

    // Delete namespaces
    deleteNamespace(NamespaceId.DEFAULT);
    deleteNamespace(NAMESPACE_ID);
    deleteNamespace(OTHER_NAMESPACE_ID);
    exploreClient.close();
    exploreExecutorService.stopAndWait();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    transactionManager.stopAndWait();
  }


  /**
   * Create a namespace because app fabric is not started in explore tests.
   */
  protected static void createNamespace(NamespaceId namespaceId) throws Exception {
    namespacePathLocator.get(namespaceId).mkdirs();
    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder().setName(namespaceId).build();
    namespaceAdmin.create(namespaceMeta);
    if (!NamespaceId.DEFAULT.equals(namespaceId)) {
      QueryHandle handle = exploreService.createNamespace(namespaceMeta);
      waitForCompletionStatus(handle, 50, TimeUnit.MILLISECONDS, 40);
    }
  }

  /**
   * Delete a namespace because app fabric is not started in explore tests.
   */
  protected static void deleteNamespace(NamespaceId namespaceId) throws Exception {
    namespacePathLocator.get(namespaceId).delete(true);
    if (!NamespaceId.DEFAULT.equals(namespaceId)) {
      QueryHandle handle = exploreService.deleteNamespace(namespaceId);
      waitForCompletionStatus(handle, 50, TimeUnit.MILLISECONDS, 40);
    }
    namespaceAdmin.delete(namespaceId);
  }

  protected static String getDatasetHiveName(DatasetId datasetID) {
    return "dataset_" + datasetID.getEntityName().replaceAll("\\.", "_").replaceAll("-", "_");
  }

  protected static ExploreClient getExploreClient() {
    return exploreClient;
  }

  protected static QueryStatus waitForCompletionStatus(QueryHandle handle, long sleepTime,
                                                       TimeUnit timeUnit, int maxTries)
    throws ExploreException, HandleNotFoundException, InterruptedException, SQLException {
    QueryStatus status;
    int tries = 0;
    do {
      timeUnit.sleep(sleepTime);
      status = exploreService.getStatus(handle);

      if (++tries > maxTries) {
        break;
      }
    } while (!status.getStatus().isDone());
    return status;
  }

  protected static void runCommand(NamespaceId namespace, String command, boolean expectedHasResult,
                                   List<ColumnDesc> expectedColumnDescs, List<QueryResult> expectedResults)
    throws Exception {

    ListenableFuture<ExploreExecutionResult> future = exploreClient.submit(namespace, command);
    assertStatementResult(future, expectedHasResult, expectedColumnDescs, expectedResults);
  }

  protected static void assertStatementResult(ListenableFuture<ExploreExecutionResult> future,
                                              boolean expectedHasResult,
                                              @Nullable List<ColumnDesc> expectedColumnDescs,
                                              @Nullable List<QueryResult> expectedResults)
    throws Exception {

    try (ExploreExecutionResult results = future.get()) {
      Assert.assertEquals(expectedHasResult, results.hasNext());
      if (expectedColumnDescs != null) {
        Assert.assertEquals(expectedColumnDescs, results.getResultSchema());
      }
      if (expectedResults != null) {
        Assert.assertEquals(expectedResults, trimColumnValues(results));
      }
    }
  }

  protected static List<QueryResult> trimColumnValues(Iterator<QueryResult> results) {
    int i = 0;
    List<QueryResult> newResults = Lists.newArrayList();
    // Max 100 results
    while (results.hasNext() && i < 100) {
      i++;
      QueryResult result = results.next();
      List<Object> newCols = Lists.newArrayList();
      for (Object obj : result.getColumns()) {
        if (obj instanceof String) {
          newCols.add(((String) obj).trim());
        } else if (obj instanceof Double) {
          // NOTE: this means only use 4 decimals for double and float values in test cases
          newCols.add((double) Math.round((Double) obj * 10000) / 10000);
        } else {
          newCols.add(obj);
        }
      }
      newResults.add(new QueryResult(newCols));
    }
    return newResults;
  }

  private static List<Module> createInMemoryModules(CConfiguration configuration, Configuration hConf,
                                                    TemporaryFolder tmpFolder) throws IOException {
    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.MEMORY.name());
    configuration.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    configuration.set(Constants.Explore.LOCAL_DATA_DIR, tmpFolder.newFolder("hive").getAbsolutePath());
    configuration.set(TxConstants.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR, tmpFolder.newFolder("tx").getAbsolutePath());
    configuration.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, true);

    return ImmutableList.of(
      new ConfigModule(configuration, hConf),
      RemoteAuthenticatorModules.getNoOpModule(),
      new IOModule(),
      new InMemoryDiscoveryModule(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new NonCustomLocationUnitTestModule(),
      new DataSetsModules().getStandaloneModules(),
      new DataSetServiceModules().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new ExploreRuntimeModule().getInMemoryModules(),
      new ExploreClientModule(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new NamespaceAdminTestModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
          bind(MetadataServiceClient.class).to(NoOpMetadataServiceClient.class);

          // Use LocalFileTransactionStateStorage, so that we can use transaction snapshots for assertions in test
          install(Modules.override(new DataFabricModules().getInMemoryModules()).with(new AbstractModule() {
            @Override
            protected void configure() {
              bind(TransactionStateStorage.class)
                .annotatedWith(Names.named("persist"))
                .to(LocalFileTransactionStateStorage.class).in(Scopes.SINGLETON);
              bind(TransactionStateStorage.class).toProvider(TransactionStateStorageProvider.class).in(Singleton.class);
            }
          }));
        }
      }
    );
  }
}
