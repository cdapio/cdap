package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.runtime.LocationStreamFileWriterFactory;
import com.continuuity.data.stream.StreamFileWriterFactory;
import com.continuuity.data.stream.service.StreamHttpService;
import com.continuuity.data.stream.service.StreamServiceRuntimeModule;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConsumerFactory;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStoreFactory;
import com.continuuity.data2.transaction.stream.leveldb.LevelDBStreamConsumerStateStoreFactory;
import com.continuuity.data2.transaction.stream.leveldb.LevelDBStreamFileAdmin;
import com.continuuity.data2.transaction.stream.leveldb.LevelDBStreamFileConsumerFactory;
import com.continuuity.gateway.handlers.dataset.DataSetInstantiatorFromMetaData;
import com.continuuity.gateway.handlers.log.MockLogReader;
import com.continuuity.gateway.router.NettyRouter;
import com.continuuity.gateway.runtime.GatewayModule;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.logging.read.LogReader;
import com.continuuity.metrics.query.MetricsQueryService;
import com.continuuity.passport.http.client.PassportClient;
import com.continuuity.security.guice.InMemorySecurityModule;
import com.continuuity.test.internal.guice.AppFabricTestModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.util.Modules;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public abstract class GatewayTestBase {

  private static final String API_KEY = "SampleTestApiKey";
  private static final String CLUSTER = "SampleTestClusterName";
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.CONTINUUITY_API_KEY, API_KEY);

  private static Gateway gateway;
  private static final String WEBAPPSERVICE = "$HOST";
  private static final String hostname = "127.0.0.1";
  private static int port;
  private static CConfiguration conf;

  private static Injector injector;
  private static AppFabricServer appFabricServer;
  private static NettyRouter router;
  private static EndpointStrategy endpointStrategy;
  private static MetricsQueryService metrics;
  private static StreamHttpService streamHttpService;

  @ClassRule
  public static ExternalResource resources = new ExternalResource() {

    private TemporaryFolder tmpFolder = new TemporaryFolder();

    @Override
    protected void before() throws Throwable {
      tmpFolder.create();
      conf = CConfiguration.create();
      Set<String> forwards = ImmutableSet.of("0:" + Constants.Service.GATEWAY, "0:" + WEBAPPSERVICE);
      conf.setInt(Constants.Gateway.PORT, 0);
      conf.set(Constants.Gateway.ADDRESS, hostname);
      conf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
      conf.setBoolean(Constants.Gateway.CONFIG_AUTHENTICATION_REQUIRED, true);
      conf.set(Constants.Gateway.CLUSTER_NAME, CLUSTER);
      conf.set(Constants.Router.ADDRESS, hostname);
      conf.setStrings(Constants.Router.FORWARD, forwards.toArray(new String[forwards.size()]));
      conf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
      injector = startGateway(conf);
    }

    @Override
    protected void after() {
      stopGateway(conf);
      tmpFolder.delete();
    }
  };

  public static Injector startGateway(final CConfiguration conf) {
    final Map<String, List<String>> keysAndClusters = ImmutableMap.of(API_KEY, Collections.singletonList(CLUSTER));

    // Set up our Guice injections
    injector = Guice.createInjector(
      Modules.override(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(PassportClient.class).toProvider(
              new Provider<PassportClient>() {
                @Override
                public PassportClient get() {
                  return new MockedPassportClient(keysAndClusters);
                }
              });
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
        new GatewayModule().getInMemoryModules(),
        new AppFabricTestModule(conf),
        new StreamServiceRuntimeModule().getSingleNodeModules()
      ).with(new AbstractModule() {
        @Override
        protected void configure() {
          // It's a bit hacky to add it here. Need to refactor these
          // bindings out as it overlaps with
          // AppFabricServiceModule
          bind(LogReader.class).to(MockLogReader.class).in(Scopes.SINGLETON);
          bind(DataSetInstantiatorFromMetaData.class).in(Scopes.SINGLETON);

          MockMetricsCollectionService metricsCollectionService = new
            MockMetricsCollectionService();
          bind(MetricsCollectionService.class).toInstance(metricsCollectionService);
          bind(MockMetricsCollectionService.class).toInstance(metricsCollectionService);

          bind(StreamConsumerStateStoreFactory.class)
            .to(LevelDBStreamConsumerStateStoreFactory.class).in(Singleton.class);
          bind(StreamAdmin.class).to(LevelDBStreamFileAdmin.class).in(Singleton.class);
          bind(StreamConsumerFactory.class).to(LevelDBStreamFileConsumerFactory.class).in(Singleton.class);
          bind(StreamFileWriterFactory.class).to(LocationStreamFileWriterFactory.class).in(Singleton.class);
        }
      })
    );

    gateway = injector.getInstance(Gateway.class);
    injector.getInstance(InMemoryTransactionManager.class).startAndWait();
    appFabricServer = injector.getInstance(AppFabricServer.class);
    metrics = injector.getInstance(MetricsQueryService.class);
    streamHttpService = injector.getInstance(StreamHttpService.class);
    appFabricServer.startAndWait();
    metrics.startAndWait();
    streamHttpService.startAndWait();
    gateway.startAndWait();

    // Restart handlers to check if they are resilient across restarts.
    gateway.stopAndWait();
    gateway = injector.getInstance(Gateway.class);
    gateway.startAndWait();
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
    gateway.stopAndWait();
    appFabricServer.stopAndWait();
    metrics.stopAndWait();
    streamHttpService.stopAndWait();
    router.stopAndWait();
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

}
