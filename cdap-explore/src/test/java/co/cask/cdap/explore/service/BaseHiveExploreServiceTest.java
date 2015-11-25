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

package co.cask.cdap.explore.service;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.namespace.AbstractNamespaceClient;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
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
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.service.NoOpNotificationFeedManager;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.store.DefaultNamespaceStore;
import co.cask.cdap.store.NamespaceStore;
import co.cask.http.HttpHandler;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TxConstants;
import co.cask.tephra.persist.LocalFileTransactionStateStorage;
import co.cask.tephra.persist.TransactionStateStorage;
import co.cask.tephra.runtime.TransactionStateStorageProvider;
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
import javax.ws.rs.HttpMethod;

/**
 * Base class for tests that need explore service to be running.
 */
public class BaseHiveExploreServiceTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  protected static final Id.Namespace NAMESPACE_ID = Id.Namespace.from("namespace");
  protected static final Id.Namespace OTHER_NAMESPACE_ID = Id.Namespace.from("other");
  protected static final String DEFAULT_DATABASE = "default";
  protected static final String NAMESPACE_DATABASE = "cdap_namespace";
  protected static final String OTHER_NAMESPACE_DATABASE = "cdap_other";
  protected static final Id.DatasetModule KEY_STRUCT_VALUE = Id.DatasetModule.from(NAMESPACE_ID, "keyStructValue");
  protected static final Id.DatasetInstance MY_TABLE = Id.DatasetInstance.from(NAMESPACE_ID, "my_table");
  protected static final String MY_TABLE_NAME = getDatasetHiveName(MY_TABLE);
  protected static final Id.DatasetModule OTHER_KEY_STRUCT_VALUE =
    Id.DatasetModule.from(OTHER_NAMESPACE_ID, "keyStructValue");
  protected static final Id.DatasetInstance OTHER_MY_TABLE = Id.DatasetInstance.from(OTHER_NAMESPACE_ID, "my_table");
  protected static final String OTHER_MY_TABLE_NAME = getDatasetHiveName(OTHER_MY_TABLE);

  // Controls for test suite for whether to run BeforeClass/AfterClass
  // Make sure to reset it back to true after using it in a test class
  public static boolean runBefore = true;
  public static boolean runAfter = true;

  protected static TransactionManager transactionManager;
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
  private static StreamAdmin streamAdmin;
  private static StreamMetaStore streamMetaStore;
  private static AbstractNamespaceClient namespaceClient;

  protected static Injector injector;

  protected static void initialize(TemporaryFolder tmpFolder) throws Exception {
    initialize(CConfiguration.create(), tmpFolder);
  }

  protected static void initialize(CConfiguration cConf, TemporaryFolder tmpFolder) throws Exception {
    initialize(cConf, tmpFolder, false);
  }

  protected static void initialize(CConfiguration cConf, TemporaryFolder tmpFolder, boolean useStandalone)
    throws Exception {
    if (!runBefore) {
      return;
    }

    Configuration hConf = new Configuration();
    List<Module> modules = useStandalone ? createStandaloneModules(cConf, hConf, tmpFolder)
      : createInMemoryModules(cConf, hConf, tmpFolder);
    injector = Guice.createInjector(modules);
    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();

    dsOpService = injector.getInstance(DatasetOpExecutor.class);
    dsOpService.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
    exploreExecutorService.startAndWait();

    datasetFramework = injector.getInstance(DatasetFramework.class);
    exploreClient = injector.getInstance(ExploreClient.class);
    exploreService = injector.getInstance(ExploreService.class);
    Assert.assertTrue(exploreClient.isServiceAvailable());

    notificationService = injector.getInstance(NotificationService.class);
    notificationService.startAndWait();

    streamService = injector.getInstance(StreamService.class);
    streamService.startAndWait();
    streamHttpService = injector.getInstance(StreamHttpService.class);
    streamHttpService.startAndWait();

    exploreTableManager = injector.getInstance(ExploreTableManager.class);

    streamAdmin = injector.getInstance(StreamAdmin.class);
    streamMetaStore = injector.getInstance(StreamMetaStore.class);
    namespaceClient = injector.getInstance(AbstractNamespaceClient.class);

    // create namespaces
    namespaceClient.create(new NamespaceMeta.Builder().setName(Id.Namespace.DEFAULT).build());
    namespaceClient.create(new NamespaceMeta.Builder().setName(NAMESPACE_ID).build());
    namespaceClient.create(new NamespaceMeta.Builder().setName(OTHER_NAMESPACE_ID).build());
    // This happens when you create a namespace via REST APIs. However, since we do not start AppFabricServer in
    // Explore tests, simulating that scenario by explicitly calling DatasetFramework APIs.
    datasetFramework.createNamespace(Id.Namespace.DEFAULT);
    datasetFramework.createNamespace(NAMESPACE_ID);
    datasetFramework.createNamespace(OTHER_NAMESPACE_ID);
  }

  @AfterClass
  public static void stopServices() throws Exception {
    if (!runAfter) {
      return;
    }

    // Delete namespaces
    namespaceClient.delete(Id.Namespace.DEFAULT);
    namespaceClient.delete(NAMESPACE_ID);
    namespaceClient.delete(OTHER_NAMESPACE_ID);
    datasetFramework.deleteNamespace(Id.Namespace.DEFAULT);
    datasetFramework.deleteNamespace(NAMESPACE_ID);
    datasetFramework.deleteNamespace(OTHER_NAMESPACE_ID);
    streamHttpService.stopAndWait();
    streamService.stopAndWait();
    notificationService.stopAndWait();
    exploreClient.close();
    exploreExecutorService.stopAndWait();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    transactionManager.stopAndWait();
  }

  protected static String getDatasetHiveName(Id.DatasetInstance datasetID) {
    return "dataset_" + datasetID.getId().replaceAll("\\.", "_").replaceAll("-", "_");
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

  protected static void runCommand(Id.Namespace namespace, String command, boolean expectedHasResult,
                                   List<ColumnDesc> expectedColumnDescs, List<QueryResult> expectedResults)
    throws Exception {

    ListenableFuture<ExploreExecutionResult> future = exploreClient.submit(namespace, command);
    assertStatementResult(future, expectedHasResult, expectedColumnDescs, expectedResults);
  }

  protected static void assertStatementResult(ListenableFuture<ExploreExecutionResult> future,
                                              boolean expectedHasResult, List<ColumnDesc> expectedColumnDescs,
                                              List<QueryResult> expectedResults)
    throws Exception {
    ExploreExecutionResult results = future.get();

    Assert.assertEquals(expectedHasResult, results.hasNext());

    Assert.assertEquals(expectedColumnDescs, results.getResultSchema());
    Assert.assertEquals(expectedResults, trimColumnValues(results));

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

  protected static void createStream(Id.Stream streamId) throws Exception {
    streamAdmin.create(streamId);
    streamMetaStore.addStream(streamId);
  }

  protected static void dropStream(Id.Stream streamId) throws Exception {
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

  protected static void sendStreamEvent(Id.Stream streamId, byte[] body) throws IOException {
    sendStreamEvent(streamId, Collections.<String, String>emptyMap(), body);
  }

  protected static void sendStreamEvent(Id.Stream streamId, Map<String, String> headers, byte[] body)
    throws IOException {
    HttpURLConnection urlConn = openStreamConnection(streamId);
    urlConn.setRequestMethod(HttpMethod.POST);
    urlConn.setDoOutput(true);
    for (Map.Entry<String, String> header : headers.entrySet()) {
      // headers must be prefixed by the stream name, otherwise they are filtered out by the StreamHandler.
      // the handler also strips the stream name from the key before writing it to the stream.
      urlConn.addRequestProperty(streamId.getId() + "." + header.getKey(), header.getValue());
    }
    urlConn.getOutputStream().write(body);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();
  }

  private static HttpURLConnection openStreamConnection(Id.Stream streamId) throws IOException {
    int port = streamHttpService.getBindAddress().getPort();
    URL url = new URL(String.format("http://127.0.0.1:%d%s/namespaces/%s/streams/%s",
                                    port, Constants.Gateway.API_VERSION_3,
                                    streamId.getNamespaceId(), streamId.getId()));
    return (HttpURLConnection) url.openConnection();
  }

  private static List<Module> createInMemoryModules(CConfiguration configuration, Configuration hConf,
                                                    TemporaryFolder tmpFolder) throws IOException {
    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.MEMORY.name());
    configuration.setBoolean(Constants.Explore.EXPLORE_ENABLED, true);
    configuration.set(Constants.Explore.LOCAL_DATA_DIR, tmpFolder.newFolder("hive").getAbsolutePath());
    configuration.set(TxConstants.Manager.CFG_TX_SNAPSHOT_LOCAL_DIR, tmpFolder.newFolder("tx").getAbsolutePath());
    configuration.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, true);

    return ImmutableList.of(
      new ConfigModule(configuration, hConf),
      new IOModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DataSetsModules().getStandaloneModules(),
      new DataSetServiceModules().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new ExploreRuntimeModule().getInMemoryModules(),
      new ExploreClientModule(),
      new StreamServiceRuntimeModule().getInMemoryModules(),
      new ViewAdminModules().getInMemoryModules(),
      new StreamAdminModules().getInMemoryModules(),
      new NotificationServiceRuntimeModule().getInMemoryModules(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(NotificationFeedManager.class).to(NoOpNotificationFeedManager.class);

          Multibinder<HttpHandler> handlerBinder =
            Multibinder.newSetBinder(binder(), HttpHandler.class, Names.named(Constants.Stream.STREAM_HANDLER));
          handlerBinder.addBinding().to(StreamHandler.class);
          handlerBinder.addBinding().to(StreamFetchHandler.class);
          handlerBinder.addBinding().to(StreamViewHttpHandler.class);
          CommonHandlers.add(handlerBinder);
          bind(NamespaceStore.class).to(DefaultNamespaceStore.class);
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
    cConf.setBoolean(Constants.Explore.EXPLORE_ENABLED, true);
    cConf.set(Constants.Explore.LOCAL_DATA_DIR, tmpFolder.newFolder("hive").getAbsolutePath());

    hConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    hConf.set(Constants.AppFabric.OUTPUT_DIR, cConf.get(Constants.AppFabric.OUTPUT_DIR));
    hConf.set("hadoop.tmp.dir", new File(localDataDir, cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsolutePath());

    return ImmutableList.of(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new DiscoveryRuntimeModule().getStandaloneModules(),
      new LocationRuntimeModule().getStandaloneModules(),
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
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(NotificationFeedManager.class).to(NoOpNotificationFeedManager.class);

          Multibinder<HttpHandler> handlerBinder =
            Multibinder.newSetBinder(binder(), HttpHandler.class, Names.named(Constants.Stream.STREAM_HANDLER));
          handlerBinder.addBinding().to(StreamHandler.class);
          handlerBinder.addBinding().to(StreamFetchHandler.class);
          CommonHandlers.add(handlerBinder);
          bind(NamespaceStore.class).to(DefaultNamespaceStore.class);
          bind(StreamHttpService.class).in(Scopes.SINGLETON);
        }
      }
    );
  }
}
