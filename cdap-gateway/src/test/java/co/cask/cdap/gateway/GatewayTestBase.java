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

package co.cask.cdap.gateway;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.gateway.handlers.log.MockLogReader;
import co.cask.cdap.gateway.router.NettyRouter;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.logging.service.LogQueryService;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.guice.SecurityModules;
import co.cask.cdap.security.impersonation.DefaultOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.spi.authorization.NoOpAuthorizer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.google.inject.util.Modules;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.tephra.TransactionManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// TODO: refactor this test. It is complete mess
public abstract class GatewayTestBase {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = new Gson();

  private static final String API_KEY = "SampleTestApiKey";
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.API_KEY, API_KEY);

  private static final String hostname = "127.0.0.1";
  private static int port;

  private static final Type RUN_RECORDS_TYPE = new TypeToken<List<RunRecord>>() { }.getType();

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
  private static LogQueryService logQueryService;
  private static MetricsQueryService metricsQueryService;
  private static MetricsCollectionService metricsCollectionService;
  private static TransactionManager txService;
  private static DatasetOpExecutor dsOpService;
  private static DatasetService datasetService;
  private static MessagingService messagingService;
  protected static NamespaceAdmin namespaceAdmin;

  private static int nestedStartCount;

  @BeforeClass
  public static void beforeClass() throws Exception {
    if (nestedStartCount++ > 0) {
      return;
    }
    conf = CConfiguration.create();
    conf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    conf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    conf.set(Constants.Router.ADDRESS, hostname);
    conf.setInt(Constants.Router.ROUTER_PORT, 0);
    injector = startGateway(conf);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (nestedStartCount-- != 0) {
      return;
    }
    stopGateway(conf);
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
        new AppFabricTestModule(conf)
      ).with(new AbstractModule() {
        @Override
        protected void configure() {
          // It's a bit hacky to add it here. Need to refactor these
          // bindings out as it overlaps with
          // AppFabricServiceModule
          bind(LogReader.class).to(MockLogReader.class).in(Scopes.SINGLETON);
          bind(PrivilegesManager.class).to(NoOpAuthorizer.class);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
        }
      })
    );

    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    txService = injector.getInstance(TransactionManager.class);
    txService.startAndWait();
    // Define all StructuredTable before starting any services that need StructuredTable
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class),
                                    injector.getInstance(StructuredTableRegistry.class));
    injector.getInstance(MetadataStore.class).createIndex();

    dsOpService = injector.getInstance(DatasetOpExecutor.class);
    dsOpService.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
    logQueryService = injector.getInstance(LogQueryService.class);
    logQueryService.startAndWait();
    metricsQueryService = injector.getInstance(MetricsQueryService.class);
    metricsQueryService.startAndWait();
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.startAndWait();
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespaceAdmin.create(TEST_NAMESPACE_META1);
    namespaceAdmin.create(TEST_NAMESPACE_META2);

    // Restart handlers to check if they are resilient across restarts.
    router = injector.getInstance(NettyRouter.class);
    router.startAndWait();
    port = router.getBoundAddress().orElseThrow(IllegalStateException::new).getPort();

    return injector;
  }

  public static void stopGateway(CConfiguration conf) throws Exception {
    namespaceAdmin.delete(new NamespaceId(TEST_NAMESPACE1));
    namespaceAdmin.delete(new NamespaceId(TEST_NAMESPACE2));
    namespaceAdmin.delete(NamespaceId.DEFAULT);
    appFabricServer.stopAndWait();
    metricsCollectionService.stopAndWait();
    metricsQueryService.stopAndWait();
    logQueryService.stopAndWait();
    router.stopAndWait();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    txService.stopAndWait();
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
    conf.clear();
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

  protected static String getState(String programType, String appId, String programId) throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doGet(String.format("/v3/namespaces/default/apps/%s/%s/%s/status",
                                                                      appId, programType, programId));
    JsonObject status = GSON.fromJson(EntityUtils.toString(response.getEntity()), JsonObject.class);
    if (status != null && status.has("status")) {
      return status.get("status").getAsString();
    }
    return null;
  }

  protected static void waitForProgramRuns(final String programType, final String appId, final String programId,
                                            final ProgramRunStatus runStatus, final int expected)
    throws Exception {

    Tasks.waitFor(expected, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        HttpResponse response = GatewayFastTestsSuite.doGet(
                                  String.format("/v3/namespaces/default/apps/%s/%s/%s/runs?status=%s&start=%d&end=%d",
                                                appId, programType, programId, runStatus, 0, Long.MAX_VALUE));
        List<RunRecord> records = GSON.fromJson(EntityUtils.toString(response.getEntity()), RUN_RECORDS_TYPE);
        return records.size();
      }
    }, 10, TimeUnit.SECONDS);
  }
}
