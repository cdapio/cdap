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
package io.cdap.cdap.gateway.handlers.metrics;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ObjectArrays;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.metrics.MapReduceMetrics;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.NoOpMetadataServiceClient;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.metrics.guice.MetricsHandlerModule;
import io.cdap.cdap.metrics.query.MetricsQueryService;
import io.cdap.cdap.security.URIScheme;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.NoOpOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.impersonation.UnsupportedUGIProvider;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.store.StoreDefinition;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.tephra.TransactionManager;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.net.InetAddress;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;

/**
 * Provides base test class for MetricsServiceTestsSuite.
 */
public abstract class MetricsSuiteTestBase {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  // Control for test suite for whether to run BeforeClass/AfterClass
  public static int nestedStartCount;

  private static final String API_KEY = "SampleTestApiKey";
  private static final String CLUSTER = "SampleTestClusterName";
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.API_KEY, API_KEY);

  private static MetricsQueryService metrics;

  protected static MetricsCollectionService collectionService;
  protected static Store store;
  protected static LocationFactory locationFactory;
  private static CConfiguration conf;

  private static Discoverable discoverable;
  private static TransactionManager transactionManager;
  private static DatasetOpExecutorService dsOpService;
  private static DatasetService datasetService;

  protected static MetricStore metricStore;

  @BeforeClass
  public static void beforeClass() throws Exception {
    if (nestedStartCount++ > 0) {
      return;
    }
    conf = CConfiguration.create();
    conf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    conf.set(Constants.Metrics.ADDRESS, InetAddress.getLoopbackAddress().getHostAddress());
    conf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    conf.setBoolean(Constants.Metrics.CONFIG_AUTHENTICATION_REQUIRED, true);
    conf.set(Constants.Metrics.CLUSTER_NAME, CLUSTER);

    Injector injector = startMetricsService(conf);
    store = injector.getInstance(Store.class);
    locationFactory = injector.getInstance(LocationFactory.class);
    metricStore = injector.getInstance(MetricStore.class);
  }

  @AfterClass
  public static void afterClass() {
    if (--nestedStartCount != 0) {
      return;
    }
    stopMetricsService(conf);
  }

  @After
  public void after() {
    metricStore.deleteAll();
  }

  public static Injector startMetricsService(CConfiguration conf) throws Exception {
    Injector injector = Guice.createInjector(Modules.override(
      new ConfigModule(conf),
      new NonCustomLocationUnitTestModule(),
      new InMemoryDiscoveryModule(),
      new MetricsHandlerModule(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getStandaloneModules(),
      new DataSetServiceModules().getInMemoryModules(),
      new ExploreClientModule(),
      new NamespaceAdminTestModule(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule()
    ).with(new AbstractModule() {
      @Override
      protected void configure() {
        bind(Store.class).to(DefaultStore.class);
        bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
        bind(OwnerAdmin.class).to(NoOpOwnerAdmin.class);
        // TODO (CDAP-14677): find a better way to inject metadata publisher
        bind(MetadataServiceClient.class).to(NoOpMetadataServiceClient.class);
      }
    }));

    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();
    StructuredTableRegistry structuredTableRegistry = injector.getInstance(StructuredTableRegistry.class);
    structuredTableRegistry.initialize();
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class), structuredTableRegistry);

    dsOpService = injector.getInstance(DatasetOpExecutorService.class);
    dsOpService.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    metrics = injector.getInstance(MetricsQueryService.class);
    metrics.startAndWait();

    collectionService = injector.getInstance(MetricsCollectionService.class);
    collectionService.startAndWait();

    // initialize the dataset instantiator
    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);

    EndpointStrategy metricsEndPoints = new RandomEndpointStrategy(
      () -> discoveryClient.discover(Constants.Service.METRICS));

    discoverable = metricsEndPoints.pick(1L, TimeUnit.SECONDS);
    Assert.assertNotNull("Could not discover metrics service", discoverable);

    return injector;
  }

  public static void stopMetricsService(CConfiguration conf) {
    collectionService.stopAndWait();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    transactionManager.stopAndWait();
    metrics.stopAndWait();
    conf.clear();
  }

  public static URI getEndPoint(String path) {
    // Replace "%" with "%%" for literal path
    return URIScheme.createURI(discoverable, path.replace("%", "%%"));
  }

  public static HttpResponse doGet(String resource) throws Exception {
    return doGet(resource, null);
  }

  public static HttpResponse doGet(String resource, Header[] headers) throws Exception {
    CloseableHttpClient client = createHttpClient();
    HttpGet get = new HttpGet(MetricsSuiteTestBase.getEndPoint(resource));
    if (headers != null) {
      get.setHeaders(ObjectArrays.concat(AUTH_HEADER, headers));
    } else {
      get.setHeader(AUTH_HEADER);
    }

    return client.execute(get);
  }

  public static HttpResponse doPost(String resource, String body) throws Exception {
    return doPost(resource, body, null);
  }

  public static HttpResponse doPost(String resource, String body, Header[] headers) throws Exception {
    CloseableHttpClient client = createHttpClient();
    HttpPost post = new HttpPost(MetricsSuiteTestBase.getEndPoint(resource));
    if (body != null) {
      post.setEntity(new StringEntity(body));
    }

    if (headers != null) {
      post.setHeaders(ObjectArrays.concat(AUTH_HEADER, headers));
    } else {
      post.setHeader(AUTH_HEADER);
    }
    return client.execute(post);
  }

  protected static Map<String, String> getServiceContext(String namespaceId, String appName, String serviceName,
                                                         String runId, String handlerName) {
    return ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.NAMESPACE, namespaceId)
      .put(Constants.Metrics.Tag.APP, appName)
      .put(Constants.Metrics.Tag.SERVICE, serviceName)
      .put(Constants.Metrics.Tag.RUN_ID, runId)
      .put(Constants.Metrics.Tag.HANDLER, handlerName).build();
  }

  protected static Map<String, String> getMapReduceTaskContext(String namespaceId, String appName, String jobName,
                                                               MapReduceMetrics.TaskType type,
                                                               String runId, String instanceId) {
    return ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.NAMESPACE, namespaceId)
      .put(Constants.Metrics.Tag.APP, appName)
      .put(Constants.Metrics.Tag.MAPREDUCE, jobName)
      .put(Constants.Metrics.Tag.MR_TASK_TYPE, type.getId())
      .put(Constants.Metrics.Tag.RUN_ID, runId)
      .put(Constants.Metrics.Tag.INSTANCE_ID, instanceId)
      .build();
  }

  protected static Map<String, String> getWorkerContext(String namespaceId, String appName, String jobName,
                                                        String runId, String instanceId) {
    return ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.NAMESPACE, namespaceId)
      .put(Constants.Metrics.Tag.APP, appName)
      .put(Constants.Metrics.Tag.WORKER, jobName)
      .put(Constants.Metrics.Tag.RUN_ID, runId)
      .put(Constants.Metrics.Tag.INSTANCE_ID, instanceId)
      .build();
  }

  private static CloseableHttpClient createHttpClient() {
    try {
      SSLContext sslContext = SSLContext.getInstance("SSL");
      sslContext.init(null, InsecureTrustManagerFactory.INSTANCE.getTrustManagers(), new SecureRandom());

      return HttpClientBuilder.create()
        .setSSLContext(sslContext)
        .setSSLHostnameVerifier((s, sslSession) -> true)
        .build();
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException(e);
    }
  }
}
