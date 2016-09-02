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

package co.cask.cdap.internal.app.services.http;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.app.store.ServiceStore;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.StreamViewClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.stream.service.StreamService;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.gateway.handlers.meta.RemoteSystemOperationsService;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.internal.test.PluginJarHelper;
import co.cask.cdap.metadata.MetadataService;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactRange;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ObjectArrays;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;
import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;

/**
 * AppFabric HttpHandler Test classes can extend this class, this will allow the HttpService be setup before
 * running the handler tests, this also gives the ability to run individual test cases.
 */
public abstract class AppFabricTestBase {
  protected static final Gson GSON = new Gson();
  private static final String API_KEY = "SampleTestApiKey";
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.API_KEY, API_KEY);

  protected static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  protected static final Type LIST_MAP_STRING_STRING_TYPE = new TypeToken<List<Map<String, String>>>() { }.getType();
  protected static final Type LIST_RUNRECORD_TYPE = new TypeToken<List<RunRecord>>() { }.getType();

  protected static final String NONEXISTENT_NAMESPACE = "12jr0j90jf3foieoi33";

  protected static final String TEST_NAMESPACE1 = "testnamespace1";
  protected static final NamespaceMeta TEST_NAMESPACE_META1 = new NamespaceMeta.Builder()
    .setName(TEST_NAMESPACE1)
    .setDescription(TEST_NAMESPACE1)
    .build();
  protected static final String TEST_NAMESPACE2 = "testnamespace2";
  protected static final NamespaceMeta TEST_NAMESPACE_META2 = new NamespaceMeta.Builder()
    .setName(TEST_NAMESPACE2)
    .setDescription(TEST_NAMESPACE2)
    .build();


  private static final String hostname = "127.0.0.1";

  private static int port;
  private static Injector injector;

  private static TransactionManager txManager;
  private static AppFabricServer appFabricServer;
  private static MetricsQueryService metricsService;
  private static MetricsCollectionService metricsCollectionService;
  private static DatasetOpExecutor dsOpService;
  private static RemoteSystemOperationsService remoteSysOpService;
  private static DatasetService datasetService;
  private static TransactionSystemClient txClient;
  private static StreamService streamService;
  private static ServiceStore serviceStore;
  private static MetadataService metadataService;
  private static LocationFactory locationFactory;
  private static StreamClient streamClient;
  private static StreamViewClient streamViewClient;
  private static DatasetClient datasetClient;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() throws Throwable {
    initializeAndStartServices(createBasicCConf(), null);
  }

  protected static void initializeAndStartServices(CConfiguration cConf,
                                                   @Nullable SConfiguration sConf) throws Exception {
    injector = Guice.createInjector(new AppFabricTestModule(cConf, sConf));

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    dsOpService = injector.getInstance(DatasetOpExecutor.class);
    dsOpService.startAndWait();
    remoteSysOpService = injector.getInstance(RemoteSystemOperationsService.class);
    remoteSysOpService.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
    ServiceDiscovered appFabricHttpDiscovered = discoveryClient.discover(Constants.Service.APP_FABRIC_HTTP);
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(appFabricHttpDiscovered);
    port = endpointStrategy.pick(1, TimeUnit.SECONDS).getSocketAddress().getPort();
    txClient = injector.getInstance(TransactionSystemClient.class);
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.startAndWait();
    metricsService = injector.getInstance(MetricsQueryService.class);
    metricsService.startAndWait();
    streamService = injector.getInstance(StreamService.class);
    streamService.startAndWait();
    serviceStore = injector.getInstance(ServiceStore.class);
    serviceStore.startAndWait();
    metadataService = injector.getInstance(MetadataService.class);
    metadataService.startAndWait();
    locationFactory = getInjector().getInstance(LocationFactory.class);
    streamClient = new StreamClient(getClientConfig(discoveryClient, Constants.Service.STREAMS));
    streamViewClient = new StreamViewClient(getClientConfig(discoveryClient, Constants.Service.STREAMS));
    datasetClient = new DatasetClient(getClientConfig(discoveryClient, Constants.Service.DATASET_MANAGER));
    createNamespaces();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    deleteNamespaces();
    streamService.stopAndWait();
    appFabricServer.stopAndWait();
    metricsCollectionService.stopAndWait();
    metricsService.stopAndWait();
    datasetService.stopAndWait();
    remoteSysOpService.stopAndWait();
    dsOpService.stopAndWait();
    txManager.stopAndWait();
    serviceStore.stopAndWait();
    metadataService.stopAndWait();
  }

  protected static CConfiguration createBasicCConf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Service.MASTER_SERVICES_BIND_ADDRESS, hostname);
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder("data").getAbsolutePath());
    cConf.set(Constants.AppFabric.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
    cConf.set(Constants.AppFabric.TEMP_DIR, System.getProperty("java.io.tmpdir"));
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    return cConf;
  }

  protected static Injector getInjector() {
    return injector;
  }

  protected static TransactionSystemClient getTxClient() {
    return txClient;
  }

  protected static int getPort() {
    return port;
  }

  protected static URI getEndPoint(String path) throws URISyntaxException {
    return new URI("http://" + hostname + ":" + port + path);
  }

  protected static HttpResponse doGet(String resource) throws Exception {
    return doGet(resource, null);
  }

  protected static HttpResponse doGet(String resource, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet(getEndPoint(resource));

    if (headers != null) {
      get.setHeaders(ObjectArrays.concat(AUTH_HEADER, headers));
    } else {
      get.setHeader(AUTH_HEADER);
    }
    return client.execute(get);
  }

  protected static HttpResponse execute(HttpUriRequest request) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    request.setHeader(AUTH_HEADER);
    return client.execute(request);
  }

  protected static HttpPost getPost(String resource) throws Exception {
    HttpPost post = new HttpPost(getEndPoint(resource));
    post.setHeader(AUTH_HEADER);
    return post;
  }

  protected static HttpPut getPut(String resource) throws Exception {
    HttpPut put = new HttpPut(getEndPoint(resource));
    put.setHeader(AUTH_HEADER);
    return put;
  }

  protected static HttpResponse doPost(String resource) throws Exception {
    return doPost(resource, null, null);
  }

  protected static HttpResponse doPost(String resource, String body) throws Exception {
    return doPost(resource, body, null);
  }

  protected static HttpResponse doPost(String resource, String body, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPost post = new HttpPost(getEndPoint(resource));

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

  protected static HttpResponse doPost(HttpPost post) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    post.setHeader(AUTH_HEADER);
    return client.execute(post);
  }

  protected static HttpResponse doPut(String resource) throws Exception {
    HttpPut put = new HttpPut(getEndPoint(resource));
    put.setHeader(AUTH_HEADER);
    return doPut(resource, null);
  }

  protected static HttpResponse doPut(String resource, String body) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(getEndPoint(resource));
    if (body != null) {
      put.setEntity(new StringEntity(body));
    }
    put.setHeader(AUTH_HEADER);
    return client.execute(put);
  }

  protected static HttpResponse doDelete(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpDelete delete = new HttpDelete(getEndPoint(resource));
    delete.setHeader(AUTH_HEADER);
    return client.execute(delete);
  }

  protected static String readResponse(HttpResponse response) throws IOException {
    HttpEntity entity = response.getEntity();
    return EntityUtils.toString(entity, "UTF-8");
  }

  protected static <T> T readResponse(HttpResponse response, Type type) throws IOException {
    return GSON.fromJson(readResponse(response), type);
  }

  protected static <T> T readResponse(HttpResponse response, Type type, Gson gson) throws IOException {
    return gson.fromJson(readResponse(response), type);
  }

  protected HttpResponse addAppArtifact(Id.Artifact artifactId, Class<?> cls) throws Exception {

    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, cls, new Manifest());

    try {
      return addArtifact(artifactId, Locations.newInputSupplier(appJar), null);
    } finally {
      appJar.delete();
    }
  }

  protected HttpResponse addPluginArtifact(Id.Artifact artifactId, Class<?> cls,
                                           Manifest manifest,
                                           Set<ArtifactRange> parents) throws Exception {

    Location appJar = PluginJarHelper.createPluginJar(locationFactory, manifest, cls);
    try {
      return addArtifact(artifactId, Locations.newInputSupplier(appJar), parents);
    } finally {
      appJar.delete();
    }
  }

  // add an artifact and return the response code
  protected HttpResponse addArtifact(Id.Artifact artifactId, InputSupplier<? extends InputStream> artifactContents,
                                     Set<ArtifactRange> parents) throws Exception {
    String path = getVersionedAPIPath("artifacts/" + artifactId.getName(), artifactId.getNamespace().getId());
    HttpEntityEnclosingRequestBase request = getPost(path);
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    request.setHeader("Artifact-Version", artifactId.getVersion().getVersion());
    if (parents != null && !parents.isEmpty()) {
      request.setHeader("Artifact-Extends", Joiner.on('/').join(parents));
    }

    request.setEntity(new ByteArrayEntity(ByteStreams.toByteArray(artifactContents)));
    return execute(request);
  }

  // add artifact properties and return the response code
  protected HttpResponse addArtifactProperties(Id.Artifact artifactId,
                                               Map<String, String> properties) throws Exception {
    String nonNamespacePath = String.format("artifacts/%s/versions/%s/properties",
                                            artifactId.getName(), artifactId.getVersion());
    String path = getVersionedAPIPath(nonNamespacePath, artifactId.getNamespace().getId());
    HttpEntityEnclosingRequestBase request = getPut(path);
    request.setEntity(new ByteArrayEntity(properties.toString().getBytes()));
    return execute(request);
  }

  /**
   * Deploys an application.
   */
  protected HttpResponse deploy(Class<?> application) throws Exception {
    return deploy(application, null, null);
  }

  protected HttpResponse deploy(Class<?> application, @Nullable String apiVersion, @Nullable String namespace)
    throws Exception {
    return deploy(application, apiVersion, namespace, null, null);
  }

  protected HttpResponse deploy(Id.Application appId,
                                AppRequest<? extends Config> appRequest) throws Exception {
    HttpEntityEnclosingRequestBase request;
    String deployPath = getVersionedAPIPath("apps/" + appId.getId(), appId.getNamespaceId());
    request = getPut(deployPath);
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    request.setHeader(HttpHeaders.Names.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    request.setEntity(new StringEntity(GSON.toJson(appRequest)));
    return execute(request);
  }

  /**
   * Deploys an application with (optionally) a defined app name and app version
   */
  protected HttpResponse deploy(Class<?> application, @Nullable String apiVersion, @Nullable String namespace,
                                @Nullable String appVersion, @Nullable Config appConfig) throws Exception {
    namespace = namespace == null ? Id.Namespace.DEFAULT.getId() : namespace;
    apiVersion = apiVersion == null ? Constants.Gateway.API_VERSION_3_TOKEN : apiVersion;
    appVersion = appVersion == null ? String.format("1.0.%d", System.currentTimeMillis()) : appVersion;

    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.BUNDLE_VERSION, appVersion);

    File artifactJar = buildAppArtifact(application, application.getSimpleName(), manifest);
    File expandDir = tmpFolder.newFolder();
    BundleJarUtil.unJar(Locations.toLocation(artifactJar), expandDir);

    // Add webapp
    File webAppFile = new File(expandDir, "webapp/default/netlens/src/1.txt");
    webAppFile.getParentFile().mkdirs();
    Files.write("dummy data", webAppFile, Charsets.UTF_8);
    BundleJarUtil.createJar(expandDir, artifactJar);

    HttpEntityEnclosingRequestBase request;
    String versionedApiPath = getVersionedAPIPath("apps/", apiVersion, namespace);
    request = getPost(versionedApiPath);
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    request.setHeader("X-Archive-Name", String.format("%s-%s.jar", application.getSimpleName(), appVersion));
    if (appConfig != null) {
      request.setHeader("X-App-Config", GSON.toJson(appConfig));
    }
    request.setEntity(new FileEntity(artifactJar));
    return execute(request);
  }

  protected String getVersionedAPIPath(String nonVersionedApiPath, String namespace) {
    return getVersionedAPIPath(nonVersionedApiPath, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
  }

  protected String getVersionedAPIPath(String nonVersionedApiPath, String version, String namespace) {
    if (!Constants.Gateway.API_VERSION_3_TOKEN.equals(version)) {
      throw new IllegalArgumentException(
        String.format("Unsupported version '%s'. Only v3 is supported.", version));
    }
    Preconditions.checkArgument(namespace != null, "Namespace cannot be null for v3 APIs.");
    return String.format("/%s/namespaces/%s/%s", version, namespace, nonVersionedApiPath);
  }

  protected List<JsonObject> getAppList(String namespace) throws Exception {
    HttpResponse response = doGet(getVersionedAPIPath("apps/", Constants.Gateway.API_VERSION_3_TOKEN, namespace));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Type typeToken = new TypeToken<List<JsonObject>>() { }.getType();
    return readResponse(response, typeToken);
  }

  protected JsonObject getAppDetails(String namespace, String appName) throws Exception {
    HttpResponse response = doGet(getVersionedAPIPath(String.format("apps/%s", appName),
                                                      Constants.Gateway.API_VERSION_3_TOKEN, namespace));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals("application/json", response.getFirstHeader(HttpHeaders.Names.CONTENT_TYPE).getValue());
    Type typeToken = new TypeToken<JsonObject>() { }.getType();
    return readResponse(response, typeToken);
  }

  protected void assertRunHistory(final Id.Program program, final String status, int expected,
                                  long timeout, TimeUnit timeoutUnit) throws Exception {
    Tasks.waitFor(expected, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return getProgramRuns(program, status).size();
      }
    }, timeout, timeoutUnit, 100, TimeUnit.MILLISECONDS);
  }

  /**
   * Checks the given schedule states.
   */
  protected void assertSchedule(final Id.Program program, final String scheduleName,
                                boolean scheduled, long timeout, TimeUnit timeoutUnit) throws Exception {
    Tasks.waitFor(scheduled, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        String statusURL = getVersionedAPIPath(String.format("apps/%s/schedules/%s/status",
            program.getApplicationId(), scheduleName),
          Constants.Gateway.API_VERSION_3_TOKEN, program.getNamespaceId());
        HttpResponse response = doGet(statusURL);
        Preconditions.checkState(200 == response.getStatusLine().getStatusCode());
        Map<String, String> result = GSON.fromJson(EntityUtils.toString(response.getEntity()),
          MAP_STRING_STRING_TYPE);
        return result != null && "SCHEDULED".equals(result.get("status"));
      }
    }, timeout, timeoutUnit, 100, TimeUnit.MILLISECONDS);
  }

  protected void deleteApp(Id.Application app, int expectedResponseCode) throws Exception {
    HttpResponse response = doDelete(getVersionedAPIPath("apps/" + app.getId(), app.getNamespaceId()));
    Assert.assertEquals(expectedResponseCode, response.getStatusLine().getStatusCode());
  }

  protected void deleteApp(final Id.Application app, int expectedResponseCode,
                           long timeout, TimeUnit timeoutUnit) throws Exception {
    Tasks.waitFor(expectedResponseCode, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        HttpResponse response = doDelete(getVersionedAPIPath("apps/" + app.getId(), app.getNamespaceId()));
        return response.getStatusLine().getStatusCode();
      }
    }, timeout, timeoutUnit, 100, TimeUnit.MILLISECONDS);
  }

  protected List<JsonObject> getArtifacts(String namespace) throws Exception {
    HttpResponse response = doGet(getVersionedAPIPath("artifacts", namespace));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Type typeToken = new TypeToken<List<JsonObject>>() { }.getType();
    return readResponse(response, typeToken);
  }

  protected void deleteArtifact(Id.Artifact artifact, int expectedResponseCode) throws Exception {
    String path = String.format("artifacts/%s/versions/%s", artifact.getName(), artifact.getVersion().getVersion());
    HttpResponse response = doDelete(getVersionedAPIPath(path, artifact.getNamespace().getId()));
    Assert.assertEquals(expectedResponseCode, response.getStatusLine().getStatusCode());
  }

  /**
   * Starts the given program.
   */
  protected void startProgram(Id.Program program) throws Exception {
    startProgram(program, 200);
  }

  /**
   * Tries to start the given program and expect the call completed with the status.
   */
  protected void startProgram(Id.Program program, int expectedStatusCode) throws Exception {
    startProgram(program, ImmutableMap.<String, String>of(), expectedStatusCode);
  }

  /**
   * Starts the given program with the given runtime arguments.
   */
  protected void startProgram(Id.Program program, Map<String, String> args) throws Exception {
    startProgram(program, args, 200);
  }

  /**
   * Tries to start the given program with the given runtime arguments and expect the call completed with the status.
   */
  protected void startProgram(Id.Program program, Map<String, String> args, int expectedStatusCode) throws Exception {
    String path = String.format("apps/%s/%s/%s/start",
                                program.getApplicationId(),
                                program.getType().getCategoryName(),
                                program.getId());
    HttpResponse response = doPost(getVersionedAPIPath(path, program.getNamespaceId()), GSON.toJson(args));
    Assert.assertEquals(expectedStatusCode, response.getStatusLine().getStatusCode());
  }

  /**
   * Tries to start the given program with the given runtime arguments and expect the call completed with the status.
   */
  protected void debugProgram(Id.Program program, int expectedStatusCode) throws Exception {
    String path = String.format("apps/%s/%s/%s/debug",
                                program.getApplicationId(),
                                program.getType().getCategoryName(),
                                program.getId());
    HttpResponse response = doPost(getVersionedAPIPath(path, program.getNamespaceId()),
                                   GSON.toJson(ImmutableMap.<String, String>of()));
    Assert.assertEquals(expectedStatusCode, response.getStatusLine().getStatusCode());
  }

  /**
   * Stops the given program.
   */
  protected void stopProgram(Id.Program program) throws Exception {
    stopProgram(program, 200);
  }

  /**
   * Tries to stop the given program and expect the call completed with the status.
   */
  protected void stopProgram(Id.Program program, int expectedStatusCode) throws Exception {
    stopProgram(program, null, expectedStatusCode);
  }

  protected void stopProgram(Id.Program program, String runId, int expectedStatusCode) throws Exception {
    stopProgram(program, runId, expectedStatusCode, null);
  }

  protected void stopProgram(Id.Program program, String runId, int expectedStatusCode, String expectedMessage)
    throws Exception {
    String path;
    if (runId == null) {
      path = String.format("apps/%s/%s/%s/stop", program.getApplicationId(), program.getType().getCategoryName(),
                           program.getId());
    } else {
      path = String.format("apps/%s/%s/%s/runs/%s/stop", program.getApplicationId(),
                           program.getType().getCategoryName(), program.getId(), runId);
    }
    HttpResponse response = doPost(getVersionedAPIPath(path, program.getNamespaceId()));
    Assert.assertEquals(expectedStatusCode, response.getStatusLine().getStatusCode());
    if (expectedMessage != null) {
      Assert.assertEquals(expectedMessage, EntityUtils.toString(response.getEntity()));
    }
  }

  /**
   * Waits for the given program to transit to the given state.
   */
  protected void waitState(final Id.Program programId, String state) throws Exception {
    Tasks.waitFor(state, new Callable<String>() {
      @Override
      public String call() throws Exception {
        String path = String.format("apps/%s/%s/%s/status",
                                    programId.getApplicationId(),
                                    programId.getType().getCategoryName(), programId.getId());
        HttpResponse response = doGet(getVersionedAPIPath(path, programId.getNamespaceId()));
        JsonObject status = GSON.fromJson(EntityUtils.toString(response.getEntity()), JsonObject.class);
        if (status == null || !status.has("status")) {
          return null;
        }
        return status.get("status").getAsString();
      }
    }, 60, TimeUnit.SECONDS);
  }

  private static void createNamespaces() throws Exception {

    HttpResponse response = doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, TEST_NAMESPACE1),
      GSON.toJson(TEST_NAMESPACE_META1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, TEST_NAMESPACE2),
      GSON.toJson(TEST_NAMESPACE_META2));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  private static void deleteNamespaces() throws Exception {
    HttpResponse response = doDelete(String.format("%s/unrecoverable/namespaces/%s", Constants.Gateway.API_VERSION_3,
                                                   TEST_NAMESPACE1));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete(String.format("%s/unrecoverable/namespaces/%s", Constants.Gateway.API_VERSION_3,
                                      TEST_NAMESPACE2));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  protected String getProgramStatus(Id.Program program) throws Exception {
    return getStatus(programStatus(program));
  }

  protected void programStatus(Id.Program program, int expectedStatus) throws Exception {
    Assert.assertEquals(expectedStatus, programStatus(program).getStatusLine().getStatusCode());
  }

  protected HttpResponse programStatus(Id.Program program) throws Exception {
    String path = String.format("apps/%s/%s/%s/status",
                                program.getApplicationId(),
                                program.getType().getCategoryName(),
                                program.getId());
    return doGet(getVersionedAPIPath(path, program.getNamespaceId()));
  }

  private String getStatus(HttpResponse response) throws Exception {
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = GSON.fromJson(s, MAP_STRING_STRING_TYPE);
    return o.get("status");
  }

  protected int suspendSchedule(String namespace, String appName, String schedule) throws Exception {
    String scheduleSuspend = String.format("apps/%s/schedules/%s/suspend", appName, schedule);
    String versionedScheduledSuspend = getVersionedAPIPath(scheduleSuspend, Constants.Gateway.API_VERSION_3_TOKEN,
      namespace);
    HttpResponse response = doPost(versionedScheduledSuspend);
    return response.getStatusLine().getStatusCode();
  }

  protected int resumeSchedule(String namespace, String appName, String schedule) throws Exception {
    String scheduleResume = String.format("apps/%s/schedules/%s/resume", appName, schedule);
    HttpResponse response = doPost(getVersionedAPIPath(scheduleResume, Constants.Gateway.API_VERSION_3_TOKEN,
                                                       namespace));
    return response.getStatusLine().getStatusCode();
  }

  protected List<ScheduleSpecification> getSchedules(String namespace, String appName, String workflowName)
    throws Exception {
    String schedulesUrl = String.format("apps/%s/workflows/%s/schedules", appName, workflowName);
    String versionedUrl = getVersionedAPIPath(schedulesUrl, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    HttpResponse response = doGet(versionedUrl);
    String json = EntityUtils.toString(response.getEntity());
    return GSON.fromJson(json, new TypeToken<List<ScheduleSpecification>>() { }.getType());
  }

  protected void verifyNoRunWithStatus(final Id.Program program, final String status) throws Exception {
    Tasks.waitFor(0, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return getProgramRuns(program, status).size();
      }
    }, 60, TimeUnit.SECONDS);
  }

  protected void verifyProgramRuns(Id.Program program, String status) throws Exception {
    verifyProgramRuns(program, status, 0);
  }

  protected void verifyProgramRuns(final Id.Program program, final String status, final int expected)
    throws Exception {
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return getProgramRuns(program, status).size() > expected;
      }
    }, 60, TimeUnit.SECONDS);
  }

  protected List<RunRecord> getProgramRuns(Id.Program program, String status) throws Exception {
    String path = String.format("apps/%s/%s/%s/runs?status=%s", program.getApplicationId(),
                                program.getType().getCategoryName(), program.getId(), status);
    HttpResponse response = doGet(getVersionedAPIPath(path, program.getNamespaceId()));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String json = EntityUtils.toString(response.getEntity());
    return GSON.fromJson(json, LIST_RUNRECORD_TYPE);
  }

  protected boolean createOrUpdateView(Id.Stream.View viewId, ViewSpecification spec) throws Exception {
    return streamViewClient.createOrUpdate(viewId, spec);
  }

  protected void deleteStream(Id.Stream stream) throws Exception {
    streamClient.delete(stream);
  }

  protected void deleteView(Id.Stream.View view) throws Exception {
    streamViewClient.delete(view);
  }

  protected void setStreamProperties(Id.Stream stream, StreamProperties props) throws Exception {
    streamClient.setStreamProperties(stream, props);
  }

  protected void updateDatasetProperties(Id.DatasetInstance dataset, Map<String, String> properties) throws Exception {
    datasetClient.updateExisting(dataset, properties);
  }

  protected void deleteDataset(Id.DatasetInstance datasetInstance) throws Exception {
    datasetClient.delete(datasetInstance);
  }

  protected HttpResponse createNamespace(String id) throws Exception {
    return doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, id));
  }

  protected HttpResponse deleteNamespace(String name) throws Exception {
    return doDelete(String.format("%s/unrecoverable/namespaces/%s", Constants.Gateway.API_VERSION_3, name));
  }

  protected HttpResponse createNamespace(String metadata, String id) throws Exception {
    return doPut(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, id), metadata);
  }

  protected HttpResponse listAllNamespaces() throws Exception {
    return doGet(String.format("%s/namespaces", Constants.Gateway.API_VERSION_3));
  }

  protected HttpResponse getNamespace(String name) throws Exception {
    Preconditions.checkArgument(name != null, "namespace name cannot be null");
    return doGet(String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3, name));
  }

  protected HttpResponse deleteNamespaceData(String name) throws Exception {
    return doDelete(String.format("%s/unrecoverable/namespaces/%s/datasets", Constants.Gateway.API_VERSION_3, name));
  }

  protected HttpResponse setProperties(String id, NamespaceMeta meta) throws Exception {
    return doPut(String.format("%s/namespaces/%s/properties", Constants.Gateway.API_VERSION_3, id),
                 GSON.toJson(meta));
  }

  protected File buildAppArtifact(Class<?> cls, String name) throws IOException {
    return buildAppArtifact(cls, name, new Manifest());
  }

  protected File buildAppArtifact(Class<?> cls, String name, Manifest manifest) throws IOException {
    if (!name.endsWith(".jar")) {
      name += ".jar";
    }
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, cls, manifest);
    File destination = new File(tmpFolder.newFolder(), name);
    Files.copy(Locations.newInputSupplier(appJar), destination);
    return destination;
  }

  private static ClientConfig getClientConfig(DiscoveryServiceClient discoveryClient, String service) {
    EndpointStrategy endpointStrategy =
      new RandomEndpointStrategy(discoveryClient.discover(service));
    Discoverable discoverable = endpointStrategy.pick(1, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverable);
    int port = discoverable.getSocketAddress().getPort();
    ConnectionConfig connectionConfig = ConnectionConfig.builder().setHostname(hostname).setPort(port).build();
    return ClientConfig.builder().setConnectionConfig(connectionConfig).build();
  }
}
