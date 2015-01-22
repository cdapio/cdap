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

package co.cask.cdap.internal.app.services.http;

import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.discovery.TimeLimitEndpointStrategy;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.cdap.test.internal.TempFolder;
import co.cask.cdap.test.internal.guice.AppFabricTestModule;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ObjectArrays;
import com.google.common.io.ByteStreams;
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
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.utils.Dependencies;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import javax.annotation.Nullable;

/**
 * AppFabric HttpHandler Test classes can extend this class, this will allow the HttpService be setup before
 * running the handler tests, this also gives the ability to run individual test cases.
 */
public abstract class AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final String API_KEY = "SampleTestApiKey";
  private static final Header AUTH_HEADER = new BasicHeader(Constants.Gateway.API_KEY, API_KEY);
  private static final String CLUSTER = "SampleTestClusterName";

  protected static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  protected static final Type LIST_MAP_STRING_STRING_TYPE = new TypeToken<List<Map<String, String>>>() { }.getType();

  private static final String hostname = "127.0.0.1";

  private static int port;
  private static Injector injector;

  private static TransactionManager txManager;
  private static AppFabricServer appFabricServer;
  private static MetricsQueryService metricsService;
  private static DatasetOpExecutor dsOpService;
  private static DatasetService datasetService;
  private static TransactionSystemClient txClient;
  private static StreamCoordinatorClient streamCoordinatorClient;
  private static final TempFolder TEMP_FOLDER = new TempFolder();
  private static final String adapterFolder = "adapter";

  @BeforeClass
  public static void beforeClass() throws Throwable {
    File adapterDir = TEMP_FOLDER.newFolder(adapterFolder);

    CConfiguration conf = CConfiguration.create();

    conf.set(Constants.AppFabric.SERVER_ADDRESS, hostname);
    conf.set(Constants.AppFabric.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
    conf.set(Constants.AppFabric.TEMP_DIR, System.getProperty("java.io.tmpdir"));

    conf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    conf.set(Constants.AppFabric.ADAPTER_DIR, adapterDir.getAbsolutePath());

    injector = Guice.createInjector(new AppFabricTestModule(conf));

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    dsOpService = injector.getInstance(DatasetOpExecutor.class);
    dsOpService.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
    DiscoveryServiceClient discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
    EndpointStrategy endpointStrategy =
      new TimeLimitEndpointStrategy(
        new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.APP_FABRIC_HTTP)),
        1L, TimeUnit.SECONDS);
    port = endpointStrategy.pick().getSocketAddress().getPort();
    txClient = injector.getInstance(TransactionSystemClient.class);
    metricsService = injector.getInstance(MetricsQueryService.class);
    metricsService.startAndWait();
    streamCoordinatorClient = injector.getInstance(StreamCoordinatorClient.class);
    streamCoordinatorClient.startAndWait();
  }

  @AfterClass
  public static void afterClass() {
    streamCoordinatorClient.stopAndWait();
    appFabricServer.stopAndWait();
    metricsService.stopAndWait();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    txManager.stopAndWait();
    TEMP_FOLDER.delete();
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
    HttpGet get = new HttpGet(AppFabricTestBase.getEndPoint(resource));

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
    HttpPost post = new HttpPost(AppFabricTestBase.getEndPoint(resource));
    post.setHeader(AUTH_HEADER);
    return post;
  }

  protected static HttpPut getPut(String resource) throws Exception {
    HttpPut put = new HttpPut(AppFabricTestBase.getEndPoint(resource));
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
    HttpPost post = new HttpPost(AppFabricTestBase.getEndPoint(resource));

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
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(AppFabricTestBase.getEndPoint(resource));
    put.setHeader(AUTH_HEADER);
    return doPut(resource, null);
  }

  protected static HttpResponse doPut(String resource, String body) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(AppFabricTestBase.getEndPoint(resource));
    if (body != null) {
      put.setEntity(new StringEntity(body));
    }
    put.setHeader(AUTH_HEADER);
    return client.execute(put);
  }

  protected static HttpResponse doDelete(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpDelete delete = new HttpDelete(AppFabricTestBase.getEndPoint(resource));
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

  /**
   * Deploys an application.
   */
  protected static HttpResponse deploy(Class<?> application) throws Exception {
    return deploy(application, null);
  }

  protected static HttpResponse deploy(Class<?> application, @Nullable String appName) throws Exception {
    return deploy(application, null, null, appName);
  }

  protected static HttpResponse deploy(Class<?> application, @Nullable String apiVersion, @Nullable String namespace)
    throws Exception {
    return deploy(application, apiVersion, namespace, null);
  }

  /**
   * Deploys an application with (optionally) a defined app name
   */
  protected static HttpResponse deploy(Class<?> application, @Nullable String apiVersion, @Nullable String namespace,
                                       @Nullable String appName) throws Exception {
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
    String versionedApiPath = getVersionedAPIPath("apps/", apiVersion, namespace);
    if (appName == null) {
      request = getPost(versionedApiPath);
    } else {
      request = getPut(versionedApiPath + appName);
    }
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    request.setHeader("X-Archive-Name", application.getSimpleName() + ".jar");
    request.setEntity(new ByteArrayEntity(bos.toByteArray()));
    return execute(request);
  }

  protected static String getVersionedAPIPath(String nonVersionedApiPath, @Nullable String version,
                                              @Nullable String namespace) {
    StringBuilder versionedApiBuilder = new StringBuilder("/");
    // if not specified, treat v2 as the version, so existing tests do not need any updates.
    if (version == null) {
      version = Constants.Gateway.API_VERSION_2_TOKEN;
    }

    if (Constants.Gateway.API_VERSION_2_TOKEN.equals(version)) {
      Preconditions.checkArgument(namespace == null,
                                  String.format("Cannot specify namespace for v2 APIs. Namespace will default to '%s'" +
                                                  " for all v2 APIs.", Constants.DEFAULT_NAMESPACE));
      versionedApiBuilder.append(version).append("/");
    } else if (Constants.Gateway.API_VERSION_3_TOKEN.equals(version)) {
      Preconditions.checkArgument(namespace != null, "Namespace cannot be null for v3 APIs.");
      versionedApiBuilder.append(version).append("/namespaces/").append(namespace).append("/");
    } else {
      throw new IllegalArgumentException(String.format("Unsupported version '%s'. Only v2 and v3 are supported.",
                                                       version));
    }
    versionedApiBuilder.append(nonVersionedApiPath);
    return versionedApiBuilder.toString();
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

  protected List<Map<String, String>> scheduleHistoryRuns(int retries, String url, int expected) throws Exception {
    int trial = 0;
    int workflowRuns = 0;
    List<Map<String, String>> history;
    String json;
    HttpResponse response;
    while (trial++ < retries) {
      response = doGet(url);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      json = EntityUtils.toString(response.getEntity());
      history = new Gson().fromJson(json, LIST_MAP_STRING_STRING_TYPE);
      workflowRuns = history.size();
      if (workflowRuns > expected) {
        return history;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue(workflowRuns > expected);
    return Lists.newArrayList();
  }

  protected void scheduleStatusCheck(int retries, String url, String expected) throws Exception {
    int trial = 0;
    String status = null;
    String json;
    Map<String, String> output;
    HttpResponse response;
    while (trial++ < retries) {
      response = doGet(url);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      json = EntityUtils.toString(response.getEntity());
      output = new Gson().fromJson(json, MAP_STRING_STRING_TYPE);
      status = output.get("status");
      if (status.equals(expected)) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertEquals(expected, status);
  }

  protected void deleteApplication(int retries, String deleteUrl, int expectedReturnCode) throws Exception {
    int trial = 0;
    HttpResponse response = null;
    while (trial++ < retries) {
      response = doDelete(deleteUrl);
      if (200 == response.getStatusLine().getStatusCode()) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertEquals(expectedReturnCode, response.getStatusLine().getStatusCode());
  }
}
