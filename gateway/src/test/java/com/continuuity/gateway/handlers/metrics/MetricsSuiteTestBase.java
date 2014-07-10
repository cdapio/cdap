/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.gateway.handlers.metrics;

import com.continuuity.api.Application;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.MockMetricsCollectionService;
import com.continuuity.gateway.MockedPassportClient;
import com.continuuity.gateway.apps.wordcount.WCount;
import com.continuuity.gateway.apps.wordcount.WordCount;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.gateway.handlers.dataset.DataSetInstantiatorFromMetaData;
import com.continuuity.gateway.handlers.log.MockLogReader;
import com.continuuity.internal.app.Specifications;
import com.continuuity.logging.read.LogReader;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.metrics.query.MetricsQueryService;
import com.continuuity.passport.http.client.PassportClient;
import com.continuuity.test.internal.guice.AppFabricTestModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
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
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.CONTINUUITY_API_KEY, API_KEY);

  private static MetricsQueryService metrics;
  private static final String hostname = "127.0.0.1";
  private static int port;

  private static File dataDir;
  private static Id.Application wordCountAppId;
  private static Id.Application wCountAppId;

  protected static MetricsCollectionService collectionService;
  protected static Store store;
  protected static LocationFactory locationFactory;
  protected static List<String> validResources;
  protected static List<String> malformedResources;
  protected static List<String> nonExistingResources;
  private static CConfiguration conf;
  private static TemporaryFolder tmpFolder;

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
    StoreFactory storeFactory = injector.getInstance(StoreFactory.class);
    store = storeFactory.create();
    locationFactory = injector.getInstance(LocationFactory.class);
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
    } catch (OperationException e) {
      e.printStackTrace();
    } finally {
      tmpFolder.delete();
    }
  }

  public static void initialize() throws IOException, OperationException {
    CConfiguration cConf = CConfiguration.create();

    // use this injector instead of the one in startMetricsService because that one uses a
    // mock metrics collection service while we need a real one.
    Injector injector = Guice.createInjector(Modules.override(
      new ConfigModule(cConf),
      new AuthModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getInMemoryModule()
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

  public static void stop() throws OperationException {
    collectionService.stopAndWait();
    store.removeApplication(wordCountAppId);
    store.removeApplication(wCountAppId);

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
    final Map<String, List<String>> keysAndClusters = ImmutableMap.of(API_KEY, Collections.singletonList(CLUSTER));

    // Set up our Guice injections
    injector = Guice.createInjector(Modules.override(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(PassportClient.class).toProvider(new Provider<PassportClient>() {
            @Override
            public PassportClient get() {
              return new MockedPassportClient(keysAndClusters);
            }
          });
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
               bind(DataSetInstantiatorFromMetaData.class).in(Scopes.SINGLETON);

               MockMetricsCollectionService metricsCollectionService =
                 new MockMetricsCollectionService();
               bind(MetricsCollectionService.class).toInstance(metricsCollectionService);
               bind(MockMetricsCollectionService.class).toInstance(metricsCollectionService);
             }
           }
    ));

    metrics = injector.getInstance(MetricsQueryService.class);
    injector.getInstance(InMemoryTransactionManager.class).startAndWait();
    metrics.startAndWait();

    // initialize the dataset instantiator
    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);

    EndpointStrategy metricsEndPoints = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.METRICS)), 1L, TimeUnit.SECONDS);

    port = metricsEndPoints.pick().getSocketAddress().getPort();

    return injector;
  }

  public static void stopMetricsService(CConfiguration conf) {
    metrics.stopAndWait();
    conf.clear();
  }

  // write WordCount app to metadata store
  public static void setupMeta() throws OperationException {

    String account = Constants.DEVELOPER_ACCOUNT_ID;
    Location appArchiveLocation = locationFactory.getHomeLocation();

    // write WordCount application to meta store
    Application app = new WordCount();
    ApplicationSpecification appSpec = Specifications.from(app.configure());
    wordCountAppId = new Id.Application(new Id.Account(account), appSpec.getName());
    store.addApplication(wordCountAppId, appSpec, appArchiveLocation);

    // write WCount application to meta store
    app = new WCount();
    appSpec = Specifications.from(app.configure());
    wCountAppId = new Id.Application(new Id.Account(account), appSpec.getName());
    store.addApplication(wCountAppId, appSpec, appArchiveLocation);

    validResources = ImmutableList.of(
      "/reactor/reads?aggregate=true",
      "/reactor/apps/WordCount/reads?aggregate=true",
      "/reactor/apps/WordCount/flows/reads?aggregate=true",
      "/reactor/apps/WordCount/flows/WordCounter/reads?aggregate=true",
      "/reactor/apps/WordCount/flows/WordCounter/flowlets/counter/reads?aggregate=true",
      "/reactor/datasets/wordStats/reads?aggregate=true",
      "/reactor/datasets/wordStats/apps/WordCount/reads?aggregate=true",
      "/reactor/datasets/wordStats/apps/WordCount/flows/WordCounter/reads?aggregate=true",
      "/reactor/datasets/wordStats/apps/WordCount/flows/WordCounter/flowlets/counter/reads?aggregate=true",
      "/reactor/streams/wordStream/collect.events?aggregate=true",
      "/reactor/cluster/resources.total.storage?aggregate=true"
    );

    malformedResources = ImmutableList.of(
      "/reacto/reads?aggregate=true",
      "/reactor/app/WordCount/reads?aggregate=true",
      "/reactor/apps/WordCount/flow/WordCounter/reads?aggregate=true",
      "/reactor/apps/WordCount/flows/WordCounter/flowlets/reads?aggregate=true",
      "/reactor/apps/WordCount/flows/WordCounter/flowlet/counter/reads?aggregate=true",
      "/reactor/dataset/wordStats/reads?aggregate=true",
      "/reactor/datasets/wordStats/app/WordCount/reads?aggregate=true",
      "/reactor/datasets/wordStats/apps/WordCount/flow/counter/reads?aggregate=true",
      "/reactor/datasets/wordStats/apps/WordCount/flows/WordCounter/flowlet/counter/reads?aggregate=true"
    );

    nonExistingResources = ImmutableList.of(
      "/reactor/apps/WordCont/reads?aggregate=true",
      "/reactor/apps/WordCount/flows/WordCouner/reads?aggregate=true",
      "/reactor/apps/WordCount/flows/WordCounter/flowlets/couter/reads?aggregate=true",
      "/reactor/datasets/wordStat/reads?aggregate=true",
      "/reactor/datasets/wordStat/apps/WordCount/reads?aggregate=true",
      "/reactor/datasets/wordStas/apps/WordCount/flows/WordCounter/reads?aggregate=true",
      "/reactor/datasets/wordStts/apps/WordCount/flows/WordCounter/flowlets/counter/reads?aggregate=true",
      "/reactor/streams/wordStrea/collect.events?aggregate=true"
    );
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

  public static HttpResponse doPut(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(MetricsSuiteTestBase.getEndPoint(resource));
    put.setHeader(AUTH_HEADER);
    return client.execute(put);
  }

  public static HttpResponse doPut(String resource, String body) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(MetricsSuiteTestBase.getEndPoint(resource));
    if (body != null) {
      put.setEntity(new StringEntity(body));
    }
    put.setHeader(AUTH_HEADER);
    return client.execute(put);
  }

  public static HttpResponse doPost(HttpPost post) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    post.setHeader(AUTH_HEADER);
    return client.execute(post);
  }

  public static HttpPost getPost(String resource) throws Exception {
    HttpPost post = new HttpPost(MetricsSuiteTestBase.getEndPoint(resource));
    post.setHeader(AUTH_HEADER);
    return post;
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

  public static HttpResponse doDelete(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpDelete delete = new HttpDelete(MetricsSuiteTestBase.getEndPoint(resource));
    delete.setHeader(AUTH_HEADER);
    return client.execute(delete);
  }
}
