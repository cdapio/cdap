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

package co.cask.cdap.gateway;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.data.runtime.LocationStreamFileWriterFactory;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.service.StreamService;
import co.cask.cdap.data.stream.service.StreamServiceRuntimeModule;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.transaction.stream.FileStreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamFileConsumerFactory;
import co.cask.cdap.gateway.handlers.log.MockLogReader;
import co.cask.cdap.gateway.router.NettyRouter;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.guice.SecurityModules;
import co.cask.cdap.security.impersonation.DefaultOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.spi.authorization.NoOpAuthorizer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.util.Modules;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.tephra.TransactionManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// TODO: refactor this test. It is complete mess
public abstract class GatewayTestBase {
  private static final Gson GSON = new Gson();

  private static final String API_KEY = "SampleTestApiKey";
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.API_KEY, API_KEY);

  private static final String hostname = "127.0.0.1";
  private static int port;

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  protected static final String TEST_NAMESPACE1 = "testnamespace1";
  protected static final NamespaceMeta TEST_NAMESPACE_META1 = new NamespaceMeta.Builder().setName(TEST_NAMESPACE1)
    .setDescription(TEST_NAMESPACE1).build();
  protected static final String TEST_NAMESPACE2 = "testnamespace2";
  protected static final NamespaceMeta TEST_NAMESPACE_META2 = new NamespaceMeta.Builder().setName(TEST_NAMESPACE2)
    .setDescription(TEST_NAMESPACE2).build();

  private static CConfiguration conf;

  private static Injector injector;
  private static AppFabricServer appFabricServer;
  private static NettyRouter router;
  private static MetricsQueryService metricsQueryService;
  private static MetricsCollectionService metricsCollectionService;
  private static TransactionManager txService;
  private static DatasetOpExecutor dsOpService;
  private static DatasetService datasetService;
  private static NotificationService notificationService;
  private static StreamService streamService;
  protected static NamespaceAdmin namespaceAdmin;
  private static TemporaryFolder tmpFolder = new TemporaryFolder();

  // Controls for test suite for whether to run BeforeClass/AfterClass
  public static boolean runBefore = true;
  public static boolean runAfter = true;

  @BeforeClass
  public static void beforeClass() throws Exception {
    if (!runBefore) {
      return;
    }
    tmpFolder.create();
    conf = CConfiguration.create();
    conf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    conf.set(Constants.Router.ADDRESS, hostname);
    conf.setInt(Constants.Router.ROUTER_PORT, 0);
    conf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    injector = startGateway(conf);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (!runAfter) {
      return;
    }
    stopGateway(conf);
    tmpFolder.delete();
  }

  public static Injector startGateway(final CConfiguration conf) throws Exception {
    // Set up our Guice injections
    injector = Guice.createInjector(
      Modules.override(
        new AbstractModule() {
          @Override
          protected void configure() {
          }

          @SuppressWarnings("unused")
          @Provides
          @Named(Constants.Router.ADDRESS)
          public final InetAddress providesHostname(CConfiguration cConf) {
            return Networks.resolve(cConf.get
              (Constants.Router.ADDRESS), new InetSocketAddress
                                      ("localhost", 0).getAddress());
          }
        },
        new SecurityModules().getInMemoryModules(),
        new NotificationServiceRuntimeModule().getInMemoryModules(),
        new AppFabricTestModule(conf)
      ).with(new AbstractModule() {
        @Override
        protected void configure() {
          install(new StreamServiceRuntimeModule().getStandaloneModules());

          // It's a bit hacky to add it here. Need to refactor these
          // bindings out as it overlaps with
          // AppFabricServiceModule
          bind(LogReader.class).to(MockLogReader.class).in(Scopes.SINGLETON);
          bind(StreamConsumerStateStoreFactory.class)
            .to(LevelDBStreamConsumerStateStoreFactory.class).in(Singleton.class);
          bind(StreamAdmin.class).to(FileStreamAdmin.class).in(Singleton.class);
          bind(StreamConsumerFactory.class).to(LevelDBStreamFileConsumerFactory.class).in(Singleton.class);
          bind(StreamFileWriterFactory.class).to(LocationStreamFileWriterFactory.class).in(Singleton.class);
          bind(PrivilegesManager.class).to(NoOpAuthorizer.class);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
        }
      })
    );

    txService = injector.getInstance(TransactionManager.class);
    txService.startAndWait();
    dsOpService = injector.getInstance(DatasetOpExecutor.class);
    dsOpService.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
    metricsQueryService = injector.getInstance(MetricsQueryService.class);
    metricsQueryService.startAndWait();
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.startAndWait();
    notificationService = injector.getInstance(NotificationService.class);
    notificationService.startAndWait();
    streamService = injector.getInstance(StreamService.class);
    streamService.startAndWait();

    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespaceAdmin.create(TEST_NAMESPACE_META1);
    namespaceAdmin.create(TEST_NAMESPACE_META2);

    // Restart handlers to check if they are resilient across restarts.
    router = injector.getInstance(NettyRouter.class);
    router.startAndWait();
    Map<String, Integer> serviceMap = Maps.newHashMap();
    for (Map.Entry<Integer, String> entry : router.getServiceLookup().getServiceMap().entrySet()) {
      serviceMap.put(entry.getValue(), entry.getKey());
    }
    port = serviceMap.get(Constants.Service.GATEWAY);

    return injector;
  }

  public static void stopGateway(CConfiguration conf) throws Exception {
    namespaceAdmin.delete(new NamespaceId(TEST_NAMESPACE1));
    namespaceAdmin.delete(new NamespaceId(TEST_NAMESPACE2));
    namespaceAdmin.delete(NamespaceId.DEFAULT);
    streamService.stopAndWait();
    notificationService.stopAndWait();
    appFabricServer.stopAndWait();
    metricsCollectionService.stopAndWait();
    metricsQueryService.stopAndWait();
    router.stopAndWait();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    txService.stopAndWait();
    conf.clear();
  }

  public static Injector getInjector() {
    return injector;
  }

  public static int getPort() {
    return port;
  }

  public static Header getAuthHeader() {
    return AUTH_HEADER;
  }

  public static URI getEndPoint(String path) throws URISyntaxException {
    return new URI("http://" + hostname + ":" + port + path);
  }

  protected static void waitState(String programType, String appId, String programId, String state) throws Exception {
    int trials = 0;
    // it may take a while for workflow/mr to start...
    while (trials++ < 20) {
      String status = getState(programType, appId, programId);
      if (status != null && state.equals(status)) {
        break;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue(trials < 20);
  }

  protected static String getState(String programType, String appId, String programId) throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doGet(String.format("/v3/namespaces/default/apps/%s/%s/%s/status",
                                                                      appId, programType, programId));
    JsonObject status = GSON.fromJson(EntityUtils.toString(response.getEntity()), JsonObject.class);
    if (status != null && status.has("status")) {
      return status.get("status").getAsString();
    }
    return null;
  }
}
