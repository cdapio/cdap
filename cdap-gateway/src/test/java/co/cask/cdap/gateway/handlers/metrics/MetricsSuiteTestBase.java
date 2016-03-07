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
package co.cask.cdap.gateway.handlers.metrics;

import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.gateway.handlers.log.MockLogReader;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsHandlerModule;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import co.cask.tephra.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Provides base test class for MetricsServiceTestsSuite.
 */
public abstract class MetricsSuiteTestBase {

  // Controls for test suite for whether to run BeforeClass/AfterClass
  public static boolean runBefore = true;
  public static boolean runAfter = true;

  private static final String API_KEY = "SampleTestApiKey";
  private static final String CLUSTER = "SampleTestClusterName";
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.API_KEY, API_KEY);

  private static MetricsQueryService metrics;
  private static final String hostname = "127.0.0.1";
  private static int port;

  private static File dataDir;

  protected static MetricsCollectionService collectionService;
  protected static Store store;
  protected static LocationFactory locationFactory;
  protected static List<String> nonExistingResources;
  private static CConfiguration conf;
  private static TemporaryFolder tmpFolder;

  private static TransactionManager transactionManager;
  private static DatasetOpExecutor dsOpService;
  private static DatasetService datasetService;

  protected static MetricStore metricStore;
  protected static LogReader logReader;

  private static Injector injector;

  @BeforeClass
  public static void beforeClass() throws Exception {
    if (!runBefore) {
      return;
    }
    tmpFolder = new TemporaryFolder();
    conf = CConfiguration.create();
    conf.set(Constants.Metrics.ADDRESS, hostname);
    conf.set(Constants.AppFabric.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
    conf.set(Constants.AppFabric.TEMP_DIR, System.getProperty("java.io.tmpdir"));
    conf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    conf.setBoolean(Constants.Metrics.CONFIG_AUTHENTICATION_REQUIRED, true);
    conf.set(Constants.Metrics.CLUSTER_NAME, CLUSTER);

    injector = startMetricsService(conf);

    store = injector.getInstance(Store.class);
    locationFactory = injector.getInstance(LocationFactory.class);
    metricStore = injector.getInstance(MetricStore.class);

    tmpFolder.create();
    dataDir = tmpFolder.newFolder();
    initialize();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (!runAfter) {
      return;
    }
    stopMetricsService(conf);
    try {
      stop();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      tmpFolder.delete();
    }
  }

  @After
  public void after() throws Exception {
    metricStore.deleteAll();
  }

  public static void initialize() throws IOException {
    CConfiguration cConf = CConfiguration.create();

    // use this injector instead of the one in startMetricsService because that one uses a
    // mock metrics collection service while we need a real one.
    Injector injector = Guice.createInjector(Modules.override(
      new ConfigModule(cConf),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new MetricsHandlerModule(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getStandaloneModules(),
      new DataSetServiceModules().getInMemoryModules(),
      new ExploreClientModule(),
      new NamespaceStoreModule().getInMemoryModules()
    ).with(new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogReader.class).to(MockLogReader.class).in(Scopes.SINGLETON);
      }
    }));

    collectionService = injector.getInstance(MetricsCollectionService.class);
    collectionService.startAndWait();
    setupMeta();
  }

  public static void stop() {
    collectionService.stopAndWait();

    Deque<File> files = Lists.newLinkedList();
    files.add(dataDir);

    File file = files.peekLast();
    while (file != null) {
      File[] children = file.listFiles();
      if (children == null || children.length == 0) {
        files.pollLast().delete();
      } else {
        Collections.addAll(files, children);
      }
      file = files.peekLast();
    }
  }

  public static Injector startMetricsService(CConfiguration conf) {
    // Set up our Guice injections
    injector = Guice.createInjector(Modules.override(
      new AbstractModule() {
        @Override
        protected void configure() {
        }
      },
      new AppFabricTestModule(conf)
    ).with(new AbstractModule() {
             @Override
             protected void configure() {
               // It's a bit hacky to add it here. Need to refactor
               // these bindings out as it overlaps with
               // AppFabricServiceModule
               bind(LogReader.class).to(MockLogReader.class).in(Scopes.SINGLETON);
             }
           }
    ));

    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();

    dsOpService = injector.getInstance(DatasetOpExecutor.class);
    dsOpService.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    metrics = injector.getInstance(MetricsQueryService.class);
    metrics.startAndWait();

    logReader = injector.getInstance(LogReader.class);

    // initialize the dataset instantiator
    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);

    EndpointStrategy metricsEndPoints = new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.METRICS));

    port = metricsEndPoints.pick(1L, TimeUnit.SECONDS).getSocketAddress().getPort();

    return injector;
  }

  public static void stopMetricsService(CConfiguration conf) {
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    transactionManager.stopAndWait();
    metrics.stopAndWait();
    conf.clear();
  }

  // write WordCount app to metadata store
  public static void setupMeta() {
    nonExistingResources = ImmutableList.of(
      "/system/apps/WordCont/reads?aggregate=true",
      "/system/apps/WordCount/flows/WordCouner/reads?aggregate=true",
      "/system/apps/WordCount/flows/WordCounter/flowlets/couter/reads?aggregate=true",
      "/system/datasets/wordStat/reads?aggregate=true",
      "/system/datasets/wordStat/apps/WordCount/reads?aggregate=true",
      "/system/datasets/wordStas/apps/WordCount/flows/WordCounter/reads?aggregate=true",
      "/system/datasets/wordStts/apps/WordCount/flows/WordCounter/flowlets/counter/reads?aggregate=true",
      "/system/streams/wordStrea/collect.events?aggregate=true"
    );
  }

  public static URI getEndPoint(String path) throws URISyntaxException {
    return new URI("http://" + hostname + ":" + port + path);
  }

  public static HttpResponse doGet(String resource) throws Exception {
    return doGet(resource, null);
  }

  public static HttpResponse doGet(String resource, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
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
    DefaultHttpClient client = new DefaultHttpClient();
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

  /**
   * Given a non-versioned API path, returns its corresponding versioned API path with v3 and default namespace.
   *
   * @param nonVersionedApiPath API path without version
   * @param namespace the namespace
   */
  public static String getVersionedAPIPath(String nonVersionedApiPath, String namespace) {
    String version = Constants.Gateway.API_VERSION_3_TOKEN;
    return String.format("/%s/namespaces/%s/%s", version, namespace, nonVersionedApiPath);
  }

  protected static Map<String, String> getFlowletContext(String namespaceId, String appName, String flowName,
                                                         String runId, String flowletName) {
    return ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.NAMESPACE, namespaceId)
      .put(Constants.Metrics.Tag.APP, appName)
      .put(Constants.Metrics.Tag.FLOW, flowName)
      .put(Constants.Metrics.Tag.RUN_ID, runId)
      .put(Constants.Metrics.Tag.FLOWLET, flowletName).build();
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
}
