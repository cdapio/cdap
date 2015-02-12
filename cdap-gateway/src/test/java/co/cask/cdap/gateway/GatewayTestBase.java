/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollectionService;
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
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.security.guice.InMemorySecurityModule;
import co.cask.cdap.test.internal.guice.AppFabricTestModule;
import co.cask.tephra.TransactionManager;
import com.google.common.collect.ImmutableMap;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// TODO: refactor this test. It is complete mess
public abstract class GatewayTestBase {
  private static final Gson GSON = new Gson();

  private static final String API_KEY = "SampleTestApiKey";
  private static final String CLUSTER = "SampleTestClusterName";
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.API_KEY, API_KEY);

  private static final String hostname = "127.0.0.1";
  private static int port;
  private static CConfiguration conf;

  private static Injector injector;
  private static AppFabricServer appFabricServer;
  private static NettyRouter router;
  private static MetricsQueryService metrics;
  private static TransactionManager txService;
  private static DatasetOpExecutor dsOpService;
  private static DatasetService datasetService;
  private static NotificationService notificationService;
  private static StreamService streamService;
  private static TemporaryFolder tmpFolder = new TemporaryFolder();

  // Controls for test suite for whether to run BeforeClass/AfterClass
  public static boolean runBefore = true;
  public static boolean runAfter = true;

  @BeforeClass
  public static void beforeClass() throws IOException {
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
  public static void afterClass() {
    if (!runAfter) {
      return;
    }
    stopGateway(conf);
    tmpFolder.delete();
  }

  public static Injector startGateway(final CConfiguration conf) {
    final Map<String, List<String>> keysAndClusters = ImmutableMap.of(API_KEY, Collections.singletonList(CLUSTER));

    // Set up our Guice injections
    injector = Guice.createInjector(
      Modules.override(
        new AbstractModule() {
          @Override
          protected void configure() {
          }

          @Provides
          @Named(Constants.Router.ADDRESS)
          public final InetAddress providesHostname(CConfiguration cConf) {
            return Networks.resolve(cConf.get
              (Constants.Router.ADDRESS), new InetSocketAddress
                                      ("localhost", 0).getAddress());
          }
        },
        new InMemorySecurityModule(),
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

          MockMetricsCollectionService metricsCollectionService = new
            MockMetricsCollectionService();
          bind(MetricsCollectionService.class).toInstance(metricsCollectionService);
          bind(MockMetricsCollectionService.class).toInstance(metricsCollectionService);

          bind(StreamConsumerStateStoreFactory.class)
            .to(LevelDBStreamConsumerStateStoreFactory.class).in(Singleton.class);
          bind(StreamAdmin.class).to(FileStreamAdmin.class).in(Singleton.class);
          bind(StreamConsumerFactory.class).to(LevelDBStreamFileConsumerFactory.class).in(Singleton.class);
          bind(StreamFileWriterFactory.class).to(LocationStreamFileWriterFactory.class).in(Singleton.class);
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
    metrics = injector.getInstance(MetricsQueryService.class);
    appFabricServer.startAndWait();
    metrics.startAndWait();
    notificationService = injector.getInstance(NotificationService.class);
    notificationService.startAndWait();
    streamService = injector.getInstance(StreamService.class);
    streamService.startAndWait();

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

  public static void stopGateway(CConfiguration conf) {
    streamService.stopAndWait();
    notificationService.stopAndWait();
    appFabricServer.stopAndWait();
    metrics.stopAndWait();
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

  protected static void waitState(String runnableType, String appId, String runnableId, String state) throws Exception {
    int trials = 0;
    // it may take a while for workflow/mr to start...
    while (trials++ < 20) {
      HttpResponse response = GatewayFastTestsSuite.doGet(String.format("/v2/apps/%s/%s/%s/status",
                                                                        appId, runnableType, runnableId));
      JsonObject status = GSON.fromJson(EntityUtils.toString(response.getEntity()), JsonObject.class);
      if (status != null && status.has("status") && state.equals(status.get("status").getAsString())) {
        break;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue(trials < 20);
  }
}
