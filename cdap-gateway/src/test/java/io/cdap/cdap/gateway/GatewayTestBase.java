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

package io.cdap.cdap.gateway;

import com.google.common.io.Closeables;
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
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.gateway.handlers.log.MockLogReader;
import io.cdap.cdap.gateway.router.NettyRouter;
import io.cdap.cdap.internal.app.services.AppFabricServer;
import io.cdap.cdap.internal.guice.AppFabricTestModule;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.logging.service.LogQueryService;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.metadata.MetadataService;
import io.cdap.cdap.metrics.query.MetricsQueryService;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.guice.SecurityModules;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.spi.authorization.NoOpAuthorizer;
import io.cdap.cdap.security.spi.authorization.PrivilegesManager;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.store.StoreDefinition;
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
  private static DatasetOpExecutorService dsOpService;
  private static DatasetService datasetService;
  private static MessagingService messagingService;
  private static MetadataService metadataService;
  private static MetadataStorage metadataStorage;
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
    metadataStorage = injector.getInstance(MetadataStorage.class);
    metadataStorage.createIndex();
    metadataService = injector.getInstance(MetadataService.class);
    metadataService.startAndWait();

    dsOpService = injector.getInstance(DatasetOpExecutorService.class);
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
    metadataService.stopAndWait();
    Closeables.closeQuietly(metadataStorage);
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
    }, 30, TimeUnit.SECONDS);
  }
}
