package com.continuuity.gateway;

import com.continuuity.app.program.ManifestFields;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.stream.service.StreamHttpModule;
import com.continuuity.data.stream.service.StreamHttpService;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.collector.NettyFlumeCollectorTest;
import com.continuuity.gateway.handlers.PingHandlerTest;
import com.continuuity.gateway.handlers.ProcedureHandlerTest;
import com.continuuity.gateway.handlers.RuntimeArgumentTest;
import com.continuuity.gateway.handlers.StreamHandlerTest;
import com.continuuity.gateway.handlers.dataset.DataSetInstantiatorFromMetaData;
import com.continuuity.gateway.handlers.hooks.MetricsReporterHookTest;
import com.continuuity.gateway.handlers.log.MockLogReader;
import com.continuuity.gateway.router.NettyRouter;
import com.continuuity.gateway.router.RouterPathTest;
import com.continuuity.gateway.runtime.GatewayModule;
import com.continuuity.gateway.tools.DataSetClientTest;
import com.continuuity.gateway.tools.StreamClientTest;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.logging.read.LogReader;
import com.continuuity.metrics.query.MetricsQueryService;
import com.continuuity.passport.http.client.PassportClient;
import com.continuuity.security.guice.InMemorySecurityModule;
import com.continuuity.test.internal.guice.AppFabricTestModule;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
import com.google.common.io.ByteStreams;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.google.inject.util.Modules;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.utils.Dependencies;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import javax.annotation.Nullable;

/**
 * Test Suite for running all API tests.
 */
@RunWith(value = Suite.class)
@Suite.SuiteClasses(value = {
  PingHandlerTest.class,
  ProcedureHandlerTest.class,
  DataSetClientTest.class,
  StreamClientTest.class,
  NettyFlumeCollectorTest.class,
  MetricsReporterHookTest.class,
  RouterPathTest.class,
  StreamHandlerTest.class,
  RuntimeArgumentTest.class
})

public class GatewayFastTestsSuite {
  private static final String API_KEY = "SampleTestApiKey";
  private static final String CLUSTER = "SampleTestClusterName";
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.CONTINUUITY_API_KEY, API_KEY);

  private static Gateway gateway;
  private static final String WEBAPPSERVICE = "$HOST";
  private static final String hostname = "127.0.0.1";
  private static int port;
  private static CConfiguration conf = CConfiguration.create();

  private static Injector injector;
  private static AppFabricServer appFabricServer;
  private static NettyRouter router;
  private static EndpointStrategy endpointStrategy;
  private static MetricsQueryService metrics;
  private static StreamHttpService streamHttpService;

  @ClassRule
  public static ExternalResource resources = new ExternalResource() {
    @Override
    protected void before() throws Throwable {
      Set<String> forwards = ImmutableSet.of("0:" + Constants.Service.GATEWAY, "0:" + WEBAPPSERVICE);
      conf.setInt(Constants.Gateway.PORT, 0);
      conf.set(Constants.Gateway.ADDRESS, hostname);
      conf.set(Constants.AppFabric.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
      conf.set(Constants.AppFabric.TEMP_DIR, System.getProperty("java.io.tmpdir"));
      conf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
      conf.set(Constants.AppFabric.SERVER_PORT, Integer.toString(Networks.getRandomPort()));
      conf.setBoolean(Constants.Gateway.CONFIG_AUTHENTICATION_REQUIRED, true);
      conf.set(Constants.Gateway.CLUSTER_NAME, CLUSTER);
      conf.set(Constants.Router.ADDRESS, hostname);
      conf.setStrings(Constants.Router.FORWARD, forwards.toArray(new String[forwards.size()]));
      injector = startGateway(conf);
    }

    @Override
    protected void after() {
      stopGateway(conf);
    }
  };

  public static Injector startGateway(CConfiguration conf) {
    final Map<String, List<String>> keysAndClusters = ImmutableMap.of(API_KEY, Collections.singletonList(CLUSTER));

    // Set up our Guice injections
    injector = Guice.createInjector(
      Modules.override(
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

          @Provides
          @Named(Constants.Router.ADDRESS)
          public final InetAddress providesHostname(CConfiguration cConf) {
            return Networks.resolve(cConf.get(Constants.Router.ADDRESS),
                                    new InetSocketAddress("localhost", 0).getAddress());
          }
        },
        new InMemorySecurityModule(),
        new GatewayModule().getInMemoryModules(),
        new AppFabricTestModule(conf),
        new StreamHttpModule()
      ).with(new AbstractModule() {
               @Override
               protected void configure() {
                 // It's a bit hacky to add it here. Need to refactor these bindings out as it overlaps with
                 // AppFabricServiceModule
                 bind(LogReader.class).to(MockLogReader.class).in(Scopes.SINGLETON);
                 bind(DataSetInstantiatorFromMetaData.class).in(Scopes.SINGLETON);

                 MockMetricsCollectionService metricsCollectionService = new MockMetricsCollectionService();
                 bind(MetricsCollectionService.class).toInstance(metricsCollectionService);
                 bind(MockMetricsCollectionService.class).toInstance(metricsCollectionService);

               }
             }
      ));

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

    // initialize the dataset instantiator
    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
    endpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.APP_FABRIC)), 1L, TimeUnit.SECONDS);
    injector.getInstance(DataSetInstantiatorFromMetaData.class).init(endpointStrategy);

    return injector;
  }

  public static void stopGateway(CConfiguration conf) {
    gateway.stopAndWait();
    appFabricServer.stopAndWait();
    metrics.stopAndWait();
    streamHttpService.stopAndWait();
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

  public static EndpointStrategy getEndpointStrategy() {
    return endpointStrategy;
  }

  public static HttpResponse doGet(String resource) throws Exception {
    return doGet(resource, null);
  }

  public static HttpResponse doGet(String resource, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet("http://" + hostname + ":" + port + resource);

    if (headers != null) {
      get.setHeaders(ObjectArrays.concat(AUTH_HEADER, headers));
    } else {

      get.setHeader(AUTH_HEADER);
    }
    return client.execute(get);
  }

  public static org.apache.http.HttpResponse doPut(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut("http://" + hostname + ":" + port + resource);
    put.setHeader(AUTH_HEADER);
    return client.execute(put);
  }

  public static HttpResponse doPut(String resource, String body) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut("http://" + hostname + ":" + port + resource);
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

  public static HttpResponse execute(HttpUriRequest request) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    request.setHeader(AUTH_HEADER);
    return client.execute(request);
  }

  public static HttpPost getPost(String resource) {
    HttpPost post = new HttpPost("http://" + hostname + ":" + port + resource);
    post.setHeader(AUTH_HEADER);
    return post;
  }

  public static HttpPut getPut(String resource) {
    HttpPut put = new HttpPut("http://" + hostname + ":" + port + resource);
    put.setHeader(AUTH_HEADER);
    return put;
  }

  public static HttpResponse doPost(String resource, String body) throws Exception {
    return doPost(resource, body, null);
  }

  public static HttpResponse doPost(String resource, String body, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPost post = new HttpPost("http://" + hostname + ":" + port + resource);

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
    HttpDelete delete = new HttpDelete("http://" + hostname + ":" + port + resource);
    delete.setHeader(AUTH_HEADER);
    return client.execute(delete);
  }

  public static HttpResponse deploy(Class<?> application, @Nullable String appName) throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, application.getName());

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    final JarOutputStream jarOut = new JarOutputStream(bos, manifest);
    final String pkgName = application.getPackage().getName();

    // Grab every classes under the application class package.
    try {
      ClassLoader classLoader = application.getClassLoader();
      if (classLoader == null) {
        classLoader = ClassLoader.getSystemClassLoader();
      }
      Dependencies.findClassDependencies(classLoader, new Dependencies.ClassAcceptor() {
        @Override
        public boolean accept(String className, URL classUrl, URL classPathUrl) {
          try {
            if (className.startsWith(pkgName)) {
              jarOut.putNextEntry(new JarEntry(className.replace('.', '/') + ".class"));
              InputStream in = classUrl.openStream();
              try {
                ByteStreams.copy(in, jarOut);
              } finally {
                in.close();
              }
              return true;
            }
            return false;
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }, application.getName());

      // Add webapp
      jarOut.putNextEntry(new ZipEntry("webapp/default/netlens/src/1.txt"));
      ByteStreams.copy(new ByteArrayInputStream("dummy data".getBytes(Charsets.UTF_8)), jarOut);
    } finally {
      jarOut.close();
    }

    HttpEntityEnclosingRequestBase request;
    if (appName == null) {
      request = getPost("/v2/apps");
    } else {
      request = getPut("/v2/apps/" + appName);
    }
    request.setHeader(Constants.Gateway.CONTINUUITY_API_KEY, "api-key-example");
    request.setHeader("X-Archive-Name", application.getSimpleName() + ".jar");
    request.setEntity(new ByteArrayEntity(bos.toByteArray()));
    return execute(request);
  }

}
