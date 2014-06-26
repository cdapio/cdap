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
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
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
 *
 */
public class MetricsSuiteTestBase {

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

  private static Injector injector;

  @ClassRule
  public static ExternalResource resources = new ExternalResource() {

    private final TemporaryFolder tmpFolder = new TemporaryFolder();

    @Override
    protected void before() throws Throwable {
      conf = CConfiguration.create();
      conf.set(Constants.Metrics.ADDRESS, hostname);
      conf.set(Constants.AppFabric.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
      conf.set(Constants.AppFabric.TEMP_DIR, System.getProperty("java.io.tmpdir"));
      conf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
      conf.setBoolean(Constants.Metrics.CONFIG_AUTHENTICATION_REQUIRED, true);
      conf.set(Constants.Metrics.CLUSTER_NAME, CLUSTER);

      injector = startMetricsService(conf);
      init(injector, tmpFolder);
    }

    @Override
    protected void after() {
      stopMetricsService(conf);
      try {
        finish(tmpFolder);
      } catch (OperationException e) {
        e.printStackTrace();
      }
    }
  };

  public static void init(Injector storeInjector, TemporaryFolder tmpFolder) throws IOException, OperationException {
    tmpFolder.create();
    dataDir = tmpFolder.newFolder();

    CConfiguration cConf = CConfiguration.create();

    // use this injector instead of the one in MetricsServiceTestsSuite because that one uses a
    // mock metrics collection service while we need a real one.
    Injector injector = Guice.createInjector(Modules.override(
      new ConfigModule(cConf),
      new AuthModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules()
    ).with(new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogReader.class).to(MockLogReader.class).in(Scopes.SINGLETON);
      }
    }));

    collectionService = injector.getInstance(MetricsCollectionService.class);
    collectionService.startAndWait();

    StoreFactory storeFactory = storeInjector.getInstance(StoreFactory.class);
    store = storeFactory.create();
    locationFactory = storeInjector.getInstance(LocationFactory.class);
    setupMeta();
  }

  public static void finish(TemporaryFolder tmpFolder) throws OperationException {
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
    tmpFolder.delete();
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
}
