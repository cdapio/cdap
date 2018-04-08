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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.EnumSet;

/**
 *  To test the RouterPathLookup regular expression tests.
 */
public class RouterPathTest {

  private static RouterPathLookup pathLookup;
  private static final HttpVersion VERSION = HttpVersion.HTTP_1_1;
  private static final String API_KEY = "SampleTestApiKey";
  private static final String FALLBACKSERVICE = "gateway";

  @BeforeClass
  public static void init() throws Exception {
    pathLookup = new RouterPathLookup();
  }

  @Test
  public void testUserServicePath() {
    for (ProgramType programType : EnumSet.of(ProgramType.SERVICE, ProgramType.SPARK)) {
      String path = "/v3/namespaces/n1/apps/a1/" + programType.getCategoryName() + "/s1/methods/m1";
      HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
      RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
      Assert.assertEquals(ServiceDiscoverable.getName("n1", "a1", programType, "s1"), result.getServiceName());
      Assert.assertTrue(ServiceDiscoverable.isUserService(result.getServiceName()));
      Assert.assertNull(result.getVersion());

      path = "/v3/namespaces/n1/apps/a1/versions/v1/" + programType.getCategoryName() + "/s1/methods/m1";
      httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
      result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
      Assert.assertEquals(ServiceDiscoverable.getName("n1", "a1", programType, "s1"), result.getServiceName());
      Assert.assertTrue(ServiceDiscoverable.isUserService(result.getServiceName()));
      Assert.assertEquals("v1", result.getVersion());
    }
  }

  @Test
  public void testSystemServicePath() {
    String path = "/v3/system/services/foo/logs";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(RouterPathLookup.METRICS, result);

    path = "/v3/system/services/foo/live-info";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    // this clashes with a rule for stream handler and fails if the rules are evaluated in wrong order [CDAP-2159]
    path = "/v3/system/services/streams/logs";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(RouterPathLookup.METRICS, result);

    // this clashes with a rule for stream handler and fails if the rules are evaluated in wrong order [CDAP-2159]
    path = "/v3/system/services/streams/live-info";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testMetricsPath() throws Exception {
    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
    String flowPath = "/v3///metrics/system/apps/InvalidApp//";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.METRICS, result);

    flowPath = "/v3/metrics";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.METRICS, result);

    flowPath = "/v3/metrics//";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.METRICS, result);

    testMetricsPath("/v3/metrics/search?target=tag&tag=namespace:user");
    testMetricsPath("/v3/metrics/search?target=tag&tag=app:PurchaeHistory&tag=flow:PurchaseFlow");
    testMetricsPath("/v3/metrics/search?target=metric&tag=app:PurchaeHistory&tag=flow:PurchaseFlow");
  }

  private void testMetricsPath(String path) {
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(RouterPathLookup.METRICS, result);
  }

  @Test
  public void testAppFabricPath() throws Exception {
    //Default destination for URIs will APP_FABRIC_HTTP
    String path = "/v3/ping/";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    path = "/status";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    path = "/v3/monitor///abcd/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), path);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testLogPath() throws Exception {
    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
    String flowPath = "/v3/namespaces/default/apps//InvalidApp///flows/FlowName/logs/";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.METRICS, result);

    flowPath = "///v3/namespaces/default///apps/InvalidApp/flows/FlowName/////logs";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.METRICS, result);

    flowPath = "/v3/namespaces/default/apps/InvalidApp/service/ServiceName/runs/7e6adc79-0f5d-4252-70817ea47698/logs/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.METRICS, result);
  }

  @Test
  public void testServicePath() throws Exception {
    // The following two should resort to resort to APP_FABRIC_HTTP, because there is no actual method being called.
    String servicePath = "v3/namespaces/default/apps/AppName/services/CatalogLookup//methods////";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), servicePath);
    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
    RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    servicePath = "v3/namespaces/some/apps/otherAppName/services/CatalogLookup//methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    // v3 servicePaths
    servicePath = "/v3/namespaces/testnamespace/apps//PurchaseHistory///services/CatalogLookup///methods//ping/1";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals("service.testnamespace.PurchaseHistory.CatalogLookup", result.getServiceName());
    Assert.assertNull(result.getVersion());

    servicePath = "///v3/namespaces/testnamespace//apps/PurchaseHistory-123//services/weird!service@@NAme///methods/" +
      "echo/someParam";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), servicePath);
    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals("service.testnamespace.PurchaseHistory-123.weird!service@@NAme", result.getServiceName());
    Assert.assertNull(result.getVersion());

    servicePath = "v3/namespaces/testnamespace/apps/SomeApp_Name/services/CatalogLookup/methods/getHistory/itemID";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals("service.testnamespace.SomeApp_Name.CatalogLookup", result.getServiceName());
    Assert.assertNull(result.getVersion());

    servicePath = "v3/namespaces/testnamespace/apps/AppName/services/CatalogLookup//methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), servicePath);
    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    servicePath = "v3/namespaces/testnamespace/apps/AppName/services/CatalogLookup////methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testStreamPath() throws Exception {
    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
    String streamPath = "/v3/namespaces/default/streams";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
    RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);

    streamPath = "///v3/namespaces/default/streams///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);

    streamPath = "v3/namespaces/default///streams///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);

    streamPath = "//v3/namespaces/default///streams/HelloStream//programs///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    streamPath = "//v3/namespaces/default///streams/HelloStream//programs///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);

    streamPath = "//v3/namespaces/default///streams/HelloStream//programs///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);

    streamPath = "v3/namespaces/default//streams//flows///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);

    streamPath = "v3/namespaces/default//streams/InvalidStreamName/programs/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    streamPath = "v3/namespaces/default//streams/InvalidStreamName/programs";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    streamPath = "v3/namespaces/default//streams/InvalidStreamName/programs/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);

    streamPath = "v3/namespaces/default//streams/InvalidStreamName/info/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, streamPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);
  }

  @Test
  public void testRouterFlowPathLookUp() throws Exception {
    String flowPath = "/v3/namespaces/default//apps/ResponseCodeAnalytics/flows/LogAnalyticsFlow/status";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testRouterWorkFlowPathLookUp() throws Exception {
    String path = "/v3/namespaces/default/apps///PurchaseHistory///workflows/PurchaseHistoryWorkflow/status";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterDeployPathLookUp() throws Exception {
    String path = "/v3/namespaces/default//apps/";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), path);
    RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterFlowletInstancesLookUp() throws Exception {
    String path = "/v3/namespaces/default//apps/WordCount/flows/WordCountFlow/flowlets/StreamSource/instances";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), path);
    RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterExplorePathLookUp() throws Exception {
    String explorePath = "/v3/namespaces/default//data///explore//datasets////mydataset//enable";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), explorePath);
    RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, explorePath, httpRequest);
    Assert.assertEquals(RouterPathLookup.EXPLORE_HTTP_USER_SERVICE, result);
  }

  @Test
  public void testRouterExploreStatusPathLookUp() throws Exception {
    String explorePath = "/v3/explore/status";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), explorePath);
    RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, explorePath, httpRequest);
    Assert.assertEquals(RouterPathLookup.EXPLORE_HTTP_USER_SERVICE, result);
  }

  @Test
  public void testRouterWebAppPathLookUp() throws Exception {
    //Calls to webapp service with appName in the first split of URI will be routed to webappService
    //But if it has v2 then use the regular router lookup logic to find the appropriate service
    final String webAppService = "webapp$HOST";
    String path = "/sentiApp/abcd/efgh///";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    RouteDestination result = pathLookup.getRoutingService(webAppService, path, httpRequest);
    Assert.assertEquals(new RouteDestination(webAppService), result);

    path = "/v3//metrics///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    result = pathLookup.getRoutingService(webAppService, path, httpRequest);
    Assert.assertEquals(RouterPathLookup.METRICS, result);
  }

  @Test
  public void testRouterV3PathLookup() {
    final String namespacePath = "/v3////namespace/////";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), namespacePath);
    RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, namespacePath, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testRouterFeedsLookup() {
    final String namespacePath = "/v3//feeds/test";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), namespacePath);
    RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, namespacePath, httpRequest);
    Assert.assertEquals(null, result);
  }

  @Test
  public void testMetadataPath() {
    // all app metadata
    assertRouting("/v3/namespaces/default//apps/WordCount//////metadata", RouterPathLookup.METADATA_SERVICE);
    // all artifact metadata
    assertRouting("/v3/namespaces/default//artifacts/WordCount///versions/v1//metadata",
                  RouterPathLookup.METADATA_SERVICE);
    // all program metadata
    assertRouting("/v3/namespaces/default//apps/WordCount//flows//WordCountFlow//metadata",
                  RouterPathLookup.METADATA_SERVICE);
    // all dataset metadata
    assertRouting("/v3/namespaces/default//datasets/ds1//////metadata", RouterPathLookup.METADATA_SERVICE);
    // all stream metadata
    assertRouting("/v3/namespaces/default//streams/s1//////metadata", RouterPathLookup.METADATA_SERVICE);
    // all stream view metadata
    assertRouting("/v3/namespaces/default//streams/s1//views/v1///metadata", RouterPathLookup.METADATA_SERVICE);
    // app metadata properties
    assertRouting("/v3/namespaces/default//apps/WordCount//////metadata///////properties",
                  RouterPathLookup.METADATA_SERVICE);
    // artifact metadata properties
    assertRouting("/v3/namespaces/default//artifacts/WordCount///versions/v1//metadata/properties",
                  RouterPathLookup.METADATA_SERVICE);
    // program metadata properties
    assertRouting("/v3/namespaces/default//apps/WordCount/flows/WordCountFlow/metadata/properties"
      , RouterPathLookup.METADATA_SERVICE);
    // dataset metadata properties
    assertRouting("/v3/namespaces/default/////datasets/ds1/metadata/properties", RouterPathLookup.METADATA_SERVICE);
    // stream metadata properties
    assertRouting("/v3/namespaces////default////streams//s1/metadata/properties", RouterPathLookup.METADATA_SERVICE);
    // stream view metadata properties
    assertRouting("/v3/namespaces////default////streams//s1/views/v1/metadata/properties",
                  RouterPathLookup.METADATA_SERVICE);
    // app metadata tags
    assertRouting("/v3/namespaces/default//apps/WordCount/////metadata/tags", RouterPathLookup.METADATA_SERVICE);
    // artifact metadata tags
    assertRouting("/v3/namespaces/default//artifacts/WordCount//versions//1.0/metadata/tags",
                  RouterPathLookup.METADATA_SERVICE);
    // program metadata tags
    assertRouting("/v3/namespaces/default//apps/WordCount/flows/WordCountFlow/metadata/tags",
                  RouterPathLookup.METADATA_SERVICE);
    // dataset metadata tags
    assertRouting("/v3/namespaces/default/////datasets/ds1/metadata/tags", RouterPathLookup.METADATA_SERVICE);
    // stream metadata tags
    assertRouting("/v3/namespaces////default////streams//s1/metadata/tags", RouterPathLookup.METADATA_SERVICE);
    // stream views metadata tags
    assertRouting("/v3/namespaces////default////streams//s1//////views/////v1//metadata/tags",
                  RouterPathLookup.METADATA_SERVICE);
    // search metadata
    assertRouting("/v3/namespaces/default/metadata/search", RouterPathLookup.METADATA_SERVICE);
    // lineage
    assertRouting("/v3/namespaces/default/////datasets/ds1/lineage", RouterPathLookup.METADATA_SERVICE);
    assertRouting("/v3/namespaces/default/streams/st1/lineage", RouterPathLookup.METADATA_SERVICE);
    // get metadata for accesses
    assertRouting("/v3/namespaces/default//apps/WordCount/flows/WordCountFlow/runs/runid/metadata",
                  RouterPathLookup.METADATA_SERVICE);
  }

  @Test
  public void testAuthorizationPaths() {
    assertRouting("/v3/////security/authorization/privileges///grant", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("/v3/security/authorization/////privileges/revoke", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("/v3/security/authorization/user/alice/privileges", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("/v3/security/authorization/roles/admins////", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("/v3/security/authorization/roles/admins", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("/v3/security/authorization/roles", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("/v3/security/authorization/group/devs/roles", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("/v3/security/authorization/group/devs/roles/admins", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("//v3/security/authorization/group/devs/roles/admins", RouterPathLookup.APP_FABRIC_HTTP);
  }

  @Test
  public void testSecureStorePaths() {
    assertRouting("/v3/////namespaces/default/securekeys/key", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("/v3/namespaces////default/securekeys/key", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("/v3/namespaces/default/securekeys/key1", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("/v3/namespaces/default/securekeys/key1", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("/v3/namespaces/default/securekeys///////key1", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("/v3/namespaces/default/securekeys/", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("/v3/////namespaces/default/securekeys/", RouterPathLookup.APP_FABRIC_HTTP);
  }

  @Test
  public void testPreviewPaths() {
    assertRouting("/v3/namespaces/default/previews/preview123", RouterPathLookup.PREVIEW_HTTP);
    assertRouting("/v3/namespaces/default/previews/preview123/status", RouterPathLookup.PREVIEW_HTTP);
    assertRouting("/v3/namespaces/default/previews/preview123/stop", RouterPathLookup.PREVIEW_HTTP);
    assertRouting("/v3/namespaces/default/previews/preview123/loggers", RouterPathLookup.PREVIEW_HTTP);
    assertRouting("/v3/namespaces/default/previews/preview123/loggers/mylogger", RouterPathLookup.PREVIEW_HTTP);
    assertRouting("/v3/namespaces/default/previews/preview123/logs", RouterPathLookup.PREVIEW_HTTP);
    assertRouting("/v3/namespaces/default/previews/preview123/metrics", RouterPathLookup.PREVIEW_HTTP);
  }

  @Test
  public void testArtifactInternalsPaths() {
    assertRouting("/v3/namespaces/default/artifact-internals/artifacts", RouterPathLookup.DONT_ROUTE);
    assertRouting("/v3/namespaces/default/artifact-internals/artifact/jdbc", RouterPathLookup.DONT_ROUTE);
    assertRouting("/v3/namespaces/default/previews/artifact-internals/status", RouterPathLookup.PREVIEW_HTTP);
  }

  @Test
  public void testServiceProviderStatsPaths() {
    assertRouting("/v3/system/////serviceproviders", RouterPathLookup.APP_FABRIC_HTTP);
  }

  @Test
  public void testSystemServiceStatusPaths() {
    assertRouting(String.format("/v3/system/services/%s/status", Constants.Service.LOGSAVER),
                  RouterPathLookup.LOG_SAVER);
    assertRouting(String.format("/v3/system/services/%s/status", Constants.Service.TRANSACTION),
                  RouterPathLookup.TRANSACTION);
    assertRouting(String.format("/v3/system/services/%s/status", Constants.Service.METRICS_PROCESSOR),
                  RouterPathLookup.METRICS_PROCESSOR);
    assertRouting(String.format("/v3/system/services/%s/status", Constants.Service.METRICS),
                  RouterPathLookup.METRICS);
    assertRouting(String.format("/v3/system/services/%s/status", Constants.Service.APP_FABRIC_HTTP),
                  RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting(String.format("/v3/system/services/%s/status", Constants.Service.STREAMS),
                  RouterPathLookup.STREAMS_SERVICE);
    assertRouting(String.format("/v3/system/services/%s/status", Constants.Service.DATASET_EXECUTOR),
                  RouterPathLookup.DATASET_EXECUTOR);
    assertRouting(String.format("/v3/system/services/%s/status", Constants.Service.METADATA_SERVICE),
                  RouterPathLookup.METADATA_SERVICE);
    assertRouting(String.format("/v3/system/services/%s/status", Constants.Service.EXPLORE_HTTP_USER_SERVICE),
                  RouterPathLookup.EXPLORE_HTTP_USER_SERVICE);
    assertRouting(String.format("/v3/system/services/%s/status", Constants.Service.MESSAGING_SERVICE),
                  RouterPathLookup.MESSAGING);
    assertRouting(String.format("/v3/system/services/%s/status", "unknown.service"), null);
  }

  @Test
  public void testSystemServiceStacksPaths() {
    assertRouting(String.format("/v3/system/services/%s/stacks", Constants.Service.LOGSAVER),
                  RouterPathLookup.LOG_SAVER);
    assertRouting(String.format("/v3/system/services/%s/stacks", Constants.Service.TRANSACTION),
                  RouterPathLookup.TRANSACTION);
    assertRouting(String.format("/v3/system/services/%s/stacks", Constants.Service.METRICS_PROCESSOR),
                  RouterPathLookup.METRICS_PROCESSOR);
    assertRouting(String.format("/v3/system/services/%s/stacks", Constants.Service.METRICS),
                  RouterPathLookup.METRICS);
    assertRouting(String.format("/v3/system/services/%s/stacks", Constants.Service.APP_FABRIC_HTTP),
                  RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting(String.format("/v3/system/services/%s/stacks", Constants.Service.STREAMS),
                  RouterPathLookup.STREAMS_SERVICE);
    assertRouting(String.format("/v3/system/services/%s/stacks", Constants.Service.DATASET_EXECUTOR),
                  RouterPathLookup.DATASET_EXECUTOR);
    assertRouting(String.format("/v3/system/services/%s/stacks", Constants.Service.METADATA_SERVICE),
                  RouterPathLookup.METADATA_SERVICE);
    assertRouting(String.format("/v3/system/services/%s/stacks", Constants.Service.EXPLORE_HTTP_USER_SERVICE),
                  RouterPathLookup.EXPLORE_HTTP_USER_SERVICE);
    assertRouting(String.format("/v3/system/services/%s/stacks", Constants.Service.MESSAGING_SERVICE),
                  RouterPathLookup.MESSAGING);
    assertRouting(String.format("/v3/system/services/%s/stacks", "unknown.service"), null);
  }

  private void assertRouting(String path, RouteDestination destination) {
    for (HttpMethod method : ImmutableList.of(HttpMethod.GET, HttpMethod.POST, HttpMethod.DELETE)) {
      HttpRequest httpRequest = new DefaultHttpRequest(VERSION, method, path);
      RouteDestination result = pathLookup.getRoutingService(FALLBACKSERVICE, path, httpRequest);
      Assert.assertEquals(destination,  result);
    }
  }
}
