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

package co.cask.cdap.explore.service;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.NamespaceClientUnitTestModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.common.security.UnsupportedUGIProvider;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.StreamViewHttpHandler;
import co.cask.cdap.data.stream.service.StreamFetchHandler;
import co.cask.cdap.data.stream.service.StreamHandler;
import co.cask.cdap.data.stream.service.StreamHttpService;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data.stream.service.StreamService;
import co.cask.cdap.data.stream.service.StreamServiceRuntimeModule;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.explore.executor.ExploreExecutorService;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.explore.guice.ExploreRuntimeModule;
import co.cask.cdap.gateway.handlers.CommonHandlers;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.service.NoOpNotificationFeedManager;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationBootstrapper;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationEnforcementService;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.authorization.InMemoryAuthorizer;
import co.cask.http.HttpHandler;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
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
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.ws.rs.HttpMethod;

/**
 * Base class for tests that need explore service to be running.
 */
public class BaseHiveExploreServiceTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  protected static final NamespaceId NAMESPACE_ID = new NamespaceId("namespace");
  protected static final NamespaceId OTHER_NAMESPACE_ID = new NamespaceId("other");
  protected static final String DEFAULT_DATABASE = "default";
  protected static final String NAMESPACE_DATABASE = "cdap_namespace";
  protected static final String OTHER_NAMESPACE_DATABASE = "cdap_other";
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
  protected static DatasetOpExecutor dsOpService;
  protected static DatasetService datasetService;
  protected static ExploreExecutorService exploreExecutorService;
  protected static ExploreService exploreService;
  protected static NotificationService notificationService;
  protected static StreamHttpService streamHttpService;
  protected static StreamService streamService;
  protected static ExploreClient exploreClient;
  protected static ExploreTableManager exploreTableManager;
  protected static NamespaceAdmin namespaceAdmin;
  private static StreamAdmin streamAdmin;
  private static StreamMetaStore streamMetaStore;
  private static NamespacedLocationFactory namespacedLocationFactory;
  private static AuthorizationEnforcementService authorizationEnforcementService;

  protected static Injector injector;

  protected static void initialize(TemporaryFolder tmpFolder) throws Exception {
    initialize(CConfiguration.create(), tmpFolder);
  }

  protected static void initialize(CConfiguration cConf, TemporaryFolder tmpFolder) throws Exception {
    initialize(cConf, tmpFolder, false, false);
  }

  protected static void initialize(CConfiguration cConf, TemporaryFolder tmpFolder, boolean useStandalone,
                                   boolean enableAuthorization)
    throws Exception {
    if (!runBefore) {
      return;
    }

    Configuration hConf = new Configuration();
    if (enableAuthorization) {
      LocationFactory locationFactory = new LocalLocationFactory(tmpFolder.newFolder());
      Location authExtensionJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class);
      cConf.setBoolean(Constants.Security.ENABLED, true);
      cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
      cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, authExtensionJar.toURI().getPath());
      cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
      cConf.setBoolean(Constants.Security.Authorization.CACHE_ENABLED, false);
    }
    List<Module> modules = useStandalone ? createStandaloneModules(cConf, hConf, tmpFolder)
      : createInMemoryModules(cConf, hConf, tmpFolder);
    injector = Guice.createInjector(modules);
    authorizationEnforcementService = injector.getInstance(AuthorizationEnforcementService.class);
    if (enableAuthorization) {
      injector.getInstance(AuthorizationBootstrapper.class).run();
      authorizationEnforcementService.startAndWait();
    }
    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();
    transactionSystemClient = injector.getInstance(TransactionSystemClient.class);

    dsOpService = injector.getInstance(DatasetOpExecutor.class);
    dsOpService.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
    exploreExecutorService.startAndWait();

    datasetFramework = injector.getInstance(DatasetFramework.class);
    exploreClient = injector.getInstance(ExploreClient.class);
    exploreService = injector.getInstance(ExploreService.class);
    exploreClient.ping();

    notificationService = injector.getInstance(NotificationService.class);
    notificationService.startAndWait();

    streamService = injector.getInstance(StreamService.class);
    streamService.startAndWait();
    streamHttpService = injector.getInstance(StreamHttpService.class);
    streamHttpService.startAndWait();

    exploreTableManager = injector.getInstance(ExploreTableManager.class);

    streamAdmin = injector.getInstance(StreamAdmin.class);
    streamMetaStore = injector.getInstance(StreamMetaStore.class);

    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);

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
    streamHttpService.stopAndWait();
    streamService.stopAndWait();
    notificationService.stopAndWait();
    exploreClient.close();
    exploreExecutorService.stopAndWait();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    transactionManager.stopAndWait();
    authorizationEnforcementService.stopAndWait();
  }


  /**
   * Create a namespace because app fabric is not started in explore tests.
   */
  protected static void createNamespace(NamespaceId namespaceId) throws Exception {
    namespacedLocationFactory.get(namespaceId.toId()).mkdirs();
    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder().setName(namespaceId.toId()).build();
    namespaceAdmin.create(namespaceMeta);
    if (!NamespaceId.DEFAULT.equals(namespaceId)) {
      exploreService.createNamespace(namespaceMeta);
    }
  }

  /**
   * Delete a namespace because app fabric is not started in explore tests.
   */
  protected static void deleteNamespace(NamespaceId namespaceId) throws Exception {
    namespacedLocationFactory.get(namespaceId.toId()).delete(true);
    if (!NamespaceId.DEFAULT.equals(namespaceId)) {
      exploreService.deleteNamespace(namespaceId);
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

    ListenableFuture<ExploreExecutionResult> future = exploreClient.submit(namespace.toId(), command);
    assertStatementResult(future, expectedHasResult, expectedColumnDescs, expectedResults);
  }

  protected static void assertStatementResult(ListenableFuture<ExploreExecutionResult> future,
                                              boolean expectedHasResult,
                                              @Nullable List<ColumnDesc> expectedColumnDescs,
                                              @Nullable List<QueryResult> expectedResults)
    throws Exception {
    ExploreExecutionResult results = future.get();

    Assert.assertEquals(expectedHasResult, results.hasNext());

    if (expectedColumnDescs != null) {
      Assert.assertEquals(expectedColumnDescs, results.getResultSchema());
    }
    if (expectedResults != null) {
      Assert.assertEquals(expectedResults, trimColumnValues(results));
    }

    results.close();
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

  protected static void createStream(StreamId streamId) throws Exception {
    streamAdmin.create(streamId);
    streamMetaStore.addStream(streamId);
  }

  protected static void dropStream(StreamId streamId) throws Exception {
    streamAdmin.drop(streamId);
    streamMetaStore.removeStream(streamId);
  }

  protected static void setStreamProperties(String namespace, String streamName,
                                            StreamProperties properties) throws IOException {
    int port = streamHttpService.getBindAddress().getPort();
    URL url = new URL(String.format("http://127.0.0.1:%d%s/namespaces/%s/streams/%s/properties",
                                    port, Constants.Gateway.API_VERSION_3, namespace, streamName));
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setRequestMethod(HttpMethod.PUT);
    urlConn.setDoOutput(true);
    urlConn.getOutputStream().write(GSON.toJson(properties).getBytes(Charsets.UTF_8));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();
  }

  protected static void sendStreamEvent(StreamId streamId, byte[] body) throws IOException {
    sendStreamEvent(streamId, Collections.<String, String>emptyMap(), body);
  }

  protected static void sendStreamEvent(StreamId streamId, Map<String, String> headers, byte[] body)
    throws IOException {
    HttpURLConnection urlConn = openStreamConnection(streamId);
    urlConn.setRequestMethod(HttpMethod.POST);
    urlConn.setDoOutput(true);
    for (Map.Entry<String, String> header : headers.entrySet()) {
      // headers must be prefixed by the stream name, otherwise they are filtered out by the StreamHandler.
      // the handler also strips the stream name from the key before writing it to the stream.
      urlConn.addRequestProperty(streamId.getEntityName() + "." + header.getKey(), header.getValue());
    }
    urlConn.getOutputStream().write(body);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();
  }

  private static HttpURLConnection openStreamConnection(StreamId streamId) throws IOException {
    int port = streamHttpService.getBindAddress().getPort();
    URL url = new URL(String.format("http://127.0.0.1:%d%s/namespaces/%s/streams/%s",
                                    port, Constants.Gateway.API_VERSION_3,
                                    streamId.getNamespace(), streamId.getEntityName()));
    return (HttpURLConnection) url.openConnection();
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
      new IOModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new NonCustomLocationUnitTestModule().getModule(),
      new DataSetsModules().getStandaloneModules(),
      new DataSetServiceModules().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new ExploreRuntimeModule().getInMemoryModules(),
      new ExploreClientModule(),
      new StreamServiceRuntimeModule().getInMemoryModules(),
      new ViewAdminModules().getInMemoryModules(),
      new StreamAdminModules().getInMemoryModules(),
      new NotificationServiceRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new NamespaceClientUnitTestModule().getModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(NotificationFeedManager.class).to(NoOpNotificationFeedManager.class);
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);

          Multibinder<HttpHandler> handlerBinder =
            Multibinder.newSetBinder(binder(), HttpHandler.class, Names.named(Constants.Stream.STREAM_HANDLER));
          handlerBinder.addBinding().to(StreamHandler.class);
          handlerBinder.addBinding().to(StreamFetchHandler.class);
          handlerBinder.addBinding().to(StreamViewHttpHandler.class);
          CommonHandlers.add(handlerBinder);
          bind(StreamHttpService.class).in(Scopes.SINGLETON);

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

  // these are needed if we actually want to query streams, as the stream input format looks at the filesystem
  // to figure out splits.
  private static List<Module> createStandaloneModules(CConfiguration cConf, Configuration hConf,
                                                      TemporaryFolder tmpFolder) throws IOException {
    File localDataDir = tmpFolder.newFolder();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    cConf.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.LEVELDB.name());
    cConf.set(Constants.Explore.LOCAL_DATA_DIR, tmpFolder.newFolder("hive").getAbsolutePath());

    hConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    hConf.set(Constants.AppFabric.OUTPUT_DIR, cConf.get(Constants.AppFabric.OUTPUT_DIR));
    hConf.set("hadoop.tmp.dir", new File(localDataDir, cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsolutePath());

    return ImmutableList.of(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new DiscoveryRuntimeModule().getStandaloneModules(),
      new NonCustomLocationUnitTestModule().getModule(),
      new DataFabricModules().getStandaloneModules(),
      new DataSetsModules().getStandaloneModules(),
      new DataSetServiceModules().getStandaloneModules(),
      new MetricsClientRuntimeModule().getStandaloneModules(),
      new ExploreRuntimeModule().getStandaloneModules(),
      new ExploreClientModule(),
      new StreamServiceRuntimeModule().getStandaloneModules(),
      new ViewAdminModules().getStandaloneModules(),
      new StreamAdminModules().getStandaloneModules(),
      new NotificationServiceRuntimeModule().getStandaloneModules(),
      // Bind NamespaceClient to in memory module since the standalone module is delegating which will need a binding
      // for namespace admin to default namespace admin which needs a lot more stuff than what is needed for explore
      // unit tests. Since this explore standalone module needs persistent of files this should not affect the tests.
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(NotificationFeedManager.class).to(NoOpNotificationFeedManager.class);
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);

          Multibinder<HttpHandler> handlerBinder =
            Multibinder.newSetBinder(binder(), HttpHandler.class, Names.named(Constants.Stream.STREAM_HANDLER));
          handlerBinder.addBinding().to(StreamHandler.class);
          handlerBinder.addBinding().to(StreamFetchHandler.class);
          CommonHandlers.add(handlerBinder);
          bind(StreamHttpService.class).in(Scopes.SINGLETON);
        }
      }
    );
  }
}
