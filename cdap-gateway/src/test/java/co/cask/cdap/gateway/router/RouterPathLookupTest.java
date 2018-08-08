/*
 * Copyright © 2014-2018 Cask Data, Inc.
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
import javax.annotation.Nullable;

/**
 *  To test the RouterPathLookup regular expression tests.
 */
public class RouterPathLookupTest {

  private static RouterPathLookup pathLookup;
  private static final RouterPathFinder AUDIT_LOOK_UP = RouterPathFinder.getInstance();
  private static final HttpVersion VERSION = HttpVersion.HTTP_1_1;
  private static final String API_KEY = "SampleTestApiKey";

  @BeforeClass
  public static void init() {
    pathLookup = new RouterPathLookup();
  }

  @Test
  public void test() {
    boolean userServiceType = AUDIT_LOOK_UP.isUserServiceType("/v3/namespaces/testnamespace/apps/PurchaseHistory/services/CatalogLookup/methods/ping/1");
    Assert.assertEquals(true, userServiceType);
  }

  @Test
  public void testUserServicePath() {
    for (ProgramType programType : EnumSet.of(ProgramType.SERVICE, ProgramType.SPARK)) {
      String path = "/v3/namespaces/n1/apps/a1/" + programType.getCategoryName() + "/s1/methods/m1";
      HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
      RouteDestination result = pathLookup.getRoutingService(path, httpRequest);
      Assert.assertEquals(ServiceDiscoverable.getName("n1", "a1", programType, "s1"), result.getServiceName());
      Assert.assertTrue(ServiceDiscoverable.isUserService(result.getServiceName()));
      Assert.assertNull(result.getVersion());

      path = "/v3/namespaces/n1/apps/a1/versions/v1/" + programType.getCategoryName() + "/s1/methods/m1";
      httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
      result = pathLookup.getRoutingService(path, httpRequest);
      Assert.assertEquals(ServiceDiscoverable.getName("n1", "a1", programType, "s1"), result.getServiceName());
      Assert.assertTrue(ServiceDiscoverable.isUserService(result.getServiceName()));
      Assert.assertEquals("v1", result.getVersion());
    }
  }

  @Test
  public void testNamespaceEndpoints() throws Exception {
    // endpoints from DatasetInstanceHandler
    assertContent("/v3/namespaces/", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces/ns1", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    // test with multiple ///
    assertContent("/v3//namespaces////ns1", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces/ns1/properties", HttpMethod.PUT, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces/ns1", HttpMethod.PUT, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/unrecoverable/namespaces/ns1", HttpMethod.DELETE, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/unrecoverable/namespaces/ns1/datasets", HttpMethod.DELETE, Constants.Service.APP_FABRIC_HTTP);
  }

  @Test
  public void testSystemServicePath() {
    String path = "/v3/system/services/foo/logs";
//    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    assertContent(path, HttpMethod.GET, Constants.Service.METRICS);

    path = "/v3/system/services/foo/live-info";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    assertContent(path, HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
//    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    // this clashes with a rule for stream handler and fails if the rules are evaluated in wrong order [CDAP-2159]
    path = "/v3/system/services/streams/logs";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    assertContent(path, HttpMethod.GET, Constants.Service.METRICS);

    // this clashes with a rule for stream handler and fails if the rules are evaluated in wrong order [CDAP-2159]
    path = "/v3/system/services/streams/live-info";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    assertContent(path, HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
//    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testMetricsPath() {
    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
//    String flowPath = "/v3///metrics/system/apps/InvalidApp//";
//    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
//    assertContent(flowPath, HttpMethod.GET, Constants.Service.METRICS);
//    Assert.assertEquals(RouterPathLookup.METRICS, result);

//    flowPath = "/v3/metrics";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), flowPath);
//    assertContent(flowPath, HttpMethod.DELETE, Constants.Service.METRICS);
//    Assert.assertEquals(RouterPathLookup.METRICS, result);

//    flowPath = "/v3/metrics//";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
//    assertContent(flowPath, HttpMethod.POST, Constants.Service.METRICS);
//    Assert.assertEquals(RouterPathLookup.METRICS, result);

    testMetricsPath("/v3/metrics/search?target=tag&tag=namespace:user");
    testMetricsPath("/v3/metrics/search?target=tag&tag=app:PurchaeHistory&tag=flow:PurchaseFlow");
    testMetricsPath("/v3/metrics/search?target=metric&tag=app:PurchaeHistory&tag=flow:PurchaseFlow");
  }

  private void testMetricsPath(String path) {
//    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    assertContent(path, HttpMethod.POST, Constants.Service.METRICS);
//    Assert.assertEquals(RouterPathLookup.METRICS, result);
  }

  @Test
  public void testAppFabricPath() {
    //Default destination for URIs will APP_FABRIC_HTTP
    String path = "/v3/ping/";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    RouteDestination result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    path = "/status";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    path = "/v3/monitor///abcd/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), path);
    result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testLogPath() {
    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
    String flowPath = "/v3/namespaces/default/apps//InvalidApp///flows/FlowName/logs/";
//    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    assertContent(flowPath, HttpMethod.GET, Constants.Service.METRICS);
//    Assert.assertEquals(RouterPathLookup.METRICS, result);

//    flowPath = "///v3/namespaces/default///apps/InvalidApp/flows/FlowName/////logs";
////    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
//    assertContent(flowPath, HttpMethod.POST, Constants.Service.METRICS);
////    Assert.assertEquals(RouterPathLookup.METRICS, result);

    flowPath = "/v3/namespaces/default/apps/InvalidApp/service/ServiceName/runs/7e6adc79-0f5d-4252-70817ea47698/logs/";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    assertContent(flowPath, HttpMethod.GET, Constants.Service.METRICS);
//    Assert.assertEquals(RouterPathLookup.METRICS, result);
  }

  @Test
  public void testServicePath() {
    // The following two should resort to resort to APP_FABRIC_HTTP, because there is no actual method being called.
//    String servicePath = "v3/namespaces/default/apps/AppName/services/CatalogLookup//methods////";
//    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), servicePath);
//    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
//    assertContent(servicePath, HttpMethod.PUT, Constants.Service.APP_FABRIC_HTTP);
//    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

//    servicePath = "v3/namespaces/some/apps/otherAppName/services/CatalogLookup//methods////";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
//    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
//    assertContent(servicePath, HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
//    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    // v3 servicePaths
    String servicePath = "/v3/namespaces/testnamespace/apps//PurchaseHistory///services/CatalogLookup///methods//ping/1";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
//    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
    assertContent(servicePath, HttpMethod.GET, "service.testnamespace.PurchaseHistory.CatalogLookup");
//    Assert.assertEquals("", result.getServiceName());
//    Assert.assertNull(result.getVersion());

//    servicePath = "///v3/namespaces/testnamespace//apps/PurchaseHistory-123//services/weird!service@@NAme///methods/" +
//      "echo/someParam";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), servicePath);
//    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
//    assertContent(servicePath, HttpMethod.GET, "service.testnamespace.PurchaseHistory-123.weird!service@@NAme");
//    Assert.assertEquals("service.testnamespace.PurchaseHistory-123.weird!service@@NAme", result.getServiceName());
//    Assert.assertNull(result.getVersion());

//    servicePath = "v3/namespaces/testnamespace/apps/SomeApp_Name/services/CatalogLookup/methods/getHistory/itemID";
////    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
//    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
//    result = pathLookup.getRoutingService(servicePath, httpRequest);
//    Assert.assertEquals("service.testnamespace.SomeApp_Name.CatalogLookup", result.getServiceName());
//    Assert.assertNull(result.getVersion());
//
//    servicePath = "v3/namespaces/testnamespace/apps/AppName/services/CatalogLookup//methods////";
////    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), servicePath);
//    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
//    result = pathLookup.getRoutingService(servicePath, httpRequest);
//    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
//
//    servicePath = "v3/namespaces/testnamespace/apps/AppName/services/CatalogLookup////methods////";
////    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
//    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
//    result = pathLookup.getRoutingService(servicePath, httpRequest);
//    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testOpsDashboardPath() {
    assertContent("/v3/dashboard", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
  }

//  @Test
//  public void testStreamPath() {
//    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
//    String streamPath = "/v3/namespaces/default/streams";
////    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
//    assertContent(streamPath, HttpMethod.GET, Constants.Service.STREAMS);
////    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);
//
//    streamPath = "///v3/namespaces/default/streams///";
////    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), streamPath);
//    assertContent(streamPath, HttpMethod.GET, Constants.Service.STREAMS);
////    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);
//
//    streamPath = "/v3/namespaces/default///streams///";
////    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), streamPath);
//    assertContent(streamPath, HttpMethod.GET, Constants.Service.STREAMS);
////    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);
//
//    streamPath = "//v3/namespaces/default///streams/HelloStream//programs///";
////    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
//    assertContent(streamPath, HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
////    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
//
//    streamPath = "//v3/namespaces/default///streams/HelloStream//programs///";
////    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), streamPath);
////    assertContent(streamPath, HttpMethod.DELETE, Constants.Service.STREAMS);
////    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);
//
//    streamPath = "//v3/namespaces/default///streams/HelloStream//programs///";
////    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), streamPath);
//    assertContent(streamPath, HttpMethod.POST, Constants.Service.STREAMS);
////    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);
//
//    streamPath = "v3/namespaces/default//streams//flows///";
////    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), streamPath);
//    assertContent(streamPath, HttpMethod.GET, Constants.Service.STREAMS);
////    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);
//
//    streamPath = "/v3/namespaces/default//streams/InvalidStreamName/programs/";
////    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
//    assertContent(streamPath, HttpMethod.GET, Constants.Service.STREAMS);
////    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
//
//    streamPath = "/v3/namespaces/default//streams/InvalidStreamName/programs";
////    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
//    assertContent(streamPath, HttpMethod.GET, Constants.Service.STREAMS);
////    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
//
//    streamPath = "/v3/namespaces/default//streams/InvalidStreamName/programs/";
////    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), streamPath);
//    assertContent(streamPath, HttpMethod.GET, Constants.Service.STREAMS);
////    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);
//
//    streamPath = "/v3/namespaces/default//streams/InvalidStreamName/info/";
////    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
//    assertContent(streamPath, HttpMethod.GET, Constants.Service.STREAMS);
////    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);
//  }

  @Test
  public void testRouterFlowPathLookUp() {
    String flowPath = "/v3/namespaces/default//apps/ResponseCodeAnalytics/flows/LogAnalyticsFlow/status";
//    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    assertContent(flowPath, HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
//    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testRouterWorkFlowPathLookUp() {
    String path = "/v3/namespaces/default/apps///PurchaseHistory///workflows/PurchaseHistoryWorkflow/status";
//    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    assertContent(path, HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
//    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterDeployPathLookUp() {
    String path = "/v3/namespaces/default//apps/";
//    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), path);
    assertContent(path, HttpMethod.POST, Constants.Service.APP_FABRIC_HTTP);
//    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterFlowletInstancesLookUp() {
    String path = "/v3/namespaces/default//apps/WordCount/flows/WordCountFlow/flowlets/StreamSource/instances";
//    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), path);
    assertContent(path, HttpMethod.PUT, Constants.Service.APP_FABRIC_HTTP);
//    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterExplorePathLookUp() {
    String explorePath = "/v3/namespaces/default//data///explore//datasets////mydataset//enable";
//    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), explorePath);
    assertContent(explorePath, HttpMethod.POST, Constants.Service.EXPLORE_HTTP_USER_SERVICE);
//    Assert.assertEquals(RouterPathLookup.EXPLORE_HTTP_USER_SERVICE, result);
  }

  @Test
  public void testRouterExploreStatusPathLookUp() {
//    String explorePath = "/v3/explore/status";
//    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), explorePath);
    assertContent("/v3/explore/status", HttpMethod.GET, Constants.Service.EXPLORE_HTTP_USER_SERVICE);
//    Assert.assertEquals(RouterPathLookup.EXPLORE_HTTP_USER_SERVICE, result);
  }

  @Test
  public void testRouterV3PathLookup() {
    final String namespacePath = "/v3////namespaces/////";
//    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), namespacePath);
    assertContent(namespacePath, HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
  }

  @Test
  public void testRouterFeedsLookup() {
    final String namespacePath = "/v3//feeds/test";
//    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), namespacePath);
    assertContent(namespacePath, HttpMethod.PUT, null);
  }

  @Test
  public void testMetadataPath() {
    // all app metadata
    assertContent("/v3/namespaces/default//apps/WordCount//////metadata", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // all artifact metadata
    assertContent("/v3/namespaces/default//artifacts/WordCount///versions/v1//metadata",
                  HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // all program metadata
    assertContent("/v3/namespaces/default//apps/WordCount//flows//WordCountFlow//metadata",
                  HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // all dataset metadata
    assertContent("/v3/namespaces/default//datasets/ds1//////metadata", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // all stream metadata
    assertContent("/v3/namespaces/default//streams/s1//////metadata", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // all stream view metadata
    assertContent("/v3/namespaces/default//streams/s1//views/v1///metadata", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // app metadata properties
    assertContent("/v3/namespaces/default//apps/WordCount//////metadata///////properties",
                  HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // artifact metadata properties
    assertContent("/v3/namespaces/default//artifacts/WordCount///versions/v1//metadata/properties",
                  HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // program metadata properties
    assertContent("/v3/namespaces/default//apps/WordCount/flows/WordCountFlow/metadata/properties"
      , HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // dataset metadata properties
    assertContent("/v3/namespaces/default/////datasets/ds1/metadata/properties", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // stream metadata properties
    assertContent("/v3/namespaces////default////streams//s1/metadata/properties", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // stream view metadata properties
    assertContent("/v3/namespaces////default////streams//s1/views/v1/metadata/properties",
                  HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // app metadata tags
    assertContent("/v3/namespaces/default//apps/WordCount/////metadata/tags", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // artifact metadata tags
    assertContent("/v3/namespaces/default//artifacts/WordCount//versions//1.0/metadata/tags",
                  HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // program metadata tags
    assertContent("/v3/namespaces/default//apps/WordCount/flows/WordCountFlow/metadata/tags",
                  HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // dataset metadata tags
    assertContent("/v3/namespaces/default/////datasets/ds1/metadata/tags", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // stream metadata tags
    assertContent("/v3/namespaces////default////streams//s1/metadata/tags", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // stream views metadata tags
    assertContent("/v3/namespaces////default////streams//s1//////views/////v1//metadata/tags",
                  HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // search metadata
    assertContent("/v3/namespaces/default/metadata/search", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    assertContent("/v3/metadata/search", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // lineage
    assertContent("/v3/namespaces/default/////datasets/ds1/lineage", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    assertContent("/v3/namespaces/default/streams/st1/lineage", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    // get metadata for accesses
    assertContent("/v3/namespaces/default//apps/WordCount/flows/WordCountFlow/runs/runid/metadata",
                  HttpMethod.GET, Constants.Service.METADATA_SERVICE);

    // test field lineage path
    assertContent("/v3/namespaces/default/datasets/ds1/lineage/fields", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    assertContent("/v3/namespaces/default/datasets/ds1/lineage/fields/field1", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    assertContent("/v3/namespaces/default/datasets/ds1/lineage/fields/field1/operations", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
  }

  @Test
  public void testAuthorizationPaths() {
    assertContent("/v3/////security/authorization/privileges///grant",
                  HttpMethod.POST, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/security/authorization/////privileges/revoke",
                  HttpMethod.POST, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/security/authorization/user/alice/privileges",
                  HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/security/authorization/roles/admins////", HttpMethod.PUT, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/security/authorization/roles/admins", HttpMethod.DELETE, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/security/authorization/roles", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/security/authorization/group/devs/roles", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/security/authorization/group/devs/roles/admins",
                  HttpMethod.PUT, Constants.Service.APP_FABRIC_HTTP);
    assertContent("//v3/security/authorization/group/devs/roles/admins",
                  HttpMethod.DELETE, Constants.Service.APP_FABRIC_HTTP);
  }

  @Test
  public void testSecureStorePaths() {
    assertContent("/v3/////namespaces/default/securekeys/key", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces////default/securekeys/key", HttpMethod.PUT, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces/default/securekeys/key1", HttpMethod.DELETE, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces/default/securekeys/key1", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces/default/securekeys///////key1", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces/default/securekeys/key1/metadata", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces/default/securekeys/", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/////namespaces/default/securekeys/", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
  }

  @Test
  public void testPreviewPaths() {
    assertContent("/v3/namespaces/default/previews", HttpMethod.POST, Constants.Service.PREVIEW_HTTP);
    // there is no such endpoint defined
    assertContent("/v3/namespaces/default/previews/preview123", HttpMethod.GET, null);
    assertContent("/v3/namespaces/default/previews/preview123/status", HttpMethod.GET, Constants.Service.PREVIEW_HTTP);
    assertContent("/v3/namespaces/default/previews/preview123/stop", HttpMethod.POST, Constants.Service.PREVIEW_HTTP);
    // there is no such endpoint defined
    assertContent("/v3/namespaces/default/previews/preview123/loggers", HttpMethod.GET, null);
    // there is no such endpoint defined
    assertContent("/v3/namespaces/default/previews/preview123/loggers/mylogger", HttpMethod.GET,
                  null);
    assertContent("/v3/namespaces/default/previews/preview123/tracers",
                  HttpMethod.GET, Constants.Service.PREVIEW_HTTP);
    assertContent("/v3/namespaces/default/previews/preview123/tracers",
                  HttpMethod.POST, Constants.Service.PREVIEW_HTTP);
    assertContent("/v3/namespaces/default/previews/preview123/tracers/t1",
                  HttpMethod.GET, Constants.Service.PREVIEW_HTTP);
    assertContent("/v3/namespaces/default/previews/preview123/logs", HttpMethod.GET, Constants.Service.PREVIEW_HTTP);
    assertContent("/v3/namespaces/default/previews/preview123/logs/prev",
                  HttpMethod.GET, Constants.Service.PREVIEW_HTTP);
    assertContent("/v3/namespaces/default/previews/preview123/logs/next",
                  HttpMethod.GET, Constants.Service.PREVIEW_HTTP);
    assertContent("/v3/namespaces/default/previews/preview123/metrics/search",
                  HttpMethod.POST, Constants.Service.PREVIEW_HTTP);
    assertContent("/v3/namespaces/default/previews/preview123/metrics/query",
                  HttpMethod.POST, Constants.Service.PREVIEW_HTTP);
  }

  @Test
  public void testArtifactInternalsPaths() {
    assertContent("/v3/namespaces/default/artifact-internals/artifacts", HttpMethod.GET,
                  Constants.Router.DONT_ROUTE_SERVICE);
    assertContent("/v3/namespaces/default/artifact-internals/artifacts/jdbc/versions/1/location", HttpMethod.GET,
                  Constants.Router.DONT_ROUTE_SERVICE);
    // there is no such known endpoints defined
    assertContent("/v3/namespaces/default/previews/artifact-internals/status", HttpMethod.GET,
                  null);
  }

  @Test
  public void testServiceProviderStatsPaths() {
    assertContent("/v3/system/////serviceproviders", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/system/////serviceproviders/p1/stats", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
  }

  @Test
  public void testSystemServiceStatusPaths() {
    assertContent(String.format("/v3/system/services/%s/status", Constants.Service.LOGSAVER),
                  HttpMethod.GET, Constants.Service.LOGSAVER);
    assertContent(String.format("/v3/system/services/%s/status", Constants.Service.TRANSACTION),
                  HttpMethod.GET, Constants.Service.TRANSACTION);
    assertContent(String.format("/v3/system/services/%s/status", Constants.Service.METRICS_PROCESSOR),
                  HttpMethod.GET, Constants.Service.METRICS_PROCESSOR);
    assertContent(String.format("/v3/system/services/%s/status", Constants.Service.METRICS),
                  HttpMethod.GET, Constants.Service.METRICS);
    assertContent(String.format("/v3/system/services/%s/status", Constants.Service.APP_FABRIC_HTTP),
                  HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent(String.format("/v3/system/services/%s/status", Constants.Service.STREAMS),
                  HttpMethod.GET, Constants.Service.STREAMS);
    assertContent(String.format("/v3/system/services/%s/status", Constants.Service.DATASET_EXECUTOR),
                  HttpMethod.GET, Constants.Service.DATASET_EXECUTOR);
    assertContent(String.format("/v3/system/services/%s/status", Constants.Service.METADATA_SERVICE),
                  HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    assertContent(String.format("/v3/system/services/%s/status", Constants.Service.EXPLORE_HTTP_USER_SERVICE),
                  HttpMethod.GET, Constants.Service.EXPLORE_HTTP_USER_SERVICE);
    assertContent(String.format("/v3/system/services/%s/status", Constants.Service.MESSAGING_SERVICE),
                  HttpMethod.GET, Constants.Service.MESSAGING_SERVICE);
    assertContent(String.format("/v3/system/services/%s/status", "unknown.service"), HttpMethod.GET, null);
  }

  @Test
  public void testSystemServiceStacksPaths() {
    assertContent(String.format("/v3/system/services/%s/stacks", Constants.Service.LOGSAVER),
                  HttpMethod.GET, Constants.Service.LOGSAVER);
    assertContent(String.format("/v3/system/services/%s/stacks", Constants.Service.TRANSACTION),
                  HttpMethod.GET, Constants.Service.TRANSACTION);
    assertContent(String.format("/v3/system/services/%s/stacks", Constants.Service.METRICS_PROCESSOR),
                  HttpMethod.GET, Constants.Service.METRICS_PROCESSOR);
    assertContent(String.format("/v3/system/services/%s/stacks", Constants.Service.METRICS),
                  HttpMethod.GET, Constants.Service.METRICS);
    assertContent(String.format("/v3/system/services/%s/stacks", Constants.Service.APP_FABRIC_HTTP),
                  HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent(String.format("/v3/system/services/%s/stacks", Constants.Service.STREAMS),
                  HttpMethod.GET, Constants.Service.STREAMS);
    assertContent(String.format("/v3/system/services/%s/stacks", Constants.Service.DATASET_EXECUTOR),
                  HttpMethod.GET, Constants.Service.DATASET_EXECUTOR);
    assertContent(String.format("/v3/system/services/%s/stacks", Constants.Service.METADATA_SERVICE),
                  HttpMethod.GET, Constants.Service.METADATA_SERVICE);
    assertContent(String.format("/v3/system/services/%s/stacks", Constants.Service.EXPLORE_HTTP_USER_SERVICE),
                  HttpMethod.GET, Constants.Service.EXPLORE_HTTP_USER_SERVICE);
    assertContent(String.format("/v3/system/services/%s/stacks", Constants.Service.MESSAGING_SERVICE),
                  HttpMethod.GET, Constants.Service.MESSAGING_SERVICE);
    assertContent(String.format("/v3/system/services/%s/stacks", "unknown.service"), HttpMethod.GET, null);
  }

  @Test
  public void testProfilePaths() {
    assertContent("/v3/profiles", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/profiles/p", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/profiles/p/enable", HttpMethod.POST, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/profiles/p/disable", HttpMethod.POST, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces/default/profiles", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces/default/profiles/p", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces/default/profiles/p/enable", HttpMethod.POST, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces/default/profiles/p/disable", HttpMethod.POST, Constants.Service.APP_FABRIC_HTTP);
  }

  @Test
  public void testBeginsWith() {
    // anything begins empty sequence
    Assert.assertTrue(RouterPathLookup.beginsWith(new String[] { }));
    Assert.assertTrue(RouterPathLookup.beginsWith(new String[] { "a" }));
    // expected should not be longer than actual
    Assert.assertFalse(RouterPathLookup.beginsWith(new String[] { }, "a"));
    Assert.assertFalse(RouterPathLookup.beginsWith(new String[] { }, (String) null));
    Assert.assertFalse(RouterPathLookup.beginsWith(new String[] { "a" }, "a", "b"));
    Assert.assertFalse(RouterPathLookup.beginsWith(new String[] { "a" }, null, null));
    // prefix matches
    Assert.assertTrue(RouterPathLookup.beginsWith(new String[] { "a", "b" }, "a", "b"));
    Assert.assertTrue(RouterPathLookup.beginsWith(new String[] { "a", "b", "c" }, "a", "b"));
    // prefix with wildcards matches
    Assert.assertTrue(RouterPathLookup.beginsWith(new String[] { "a", "b" }, null, "b"));
    Assert.assertTrue(RouterPathLookup.beginsWith(new String[] { "a", "b" }, "a", null));
    Assert.assertTrue(RouterPathLookup.beginsWith(new String[] { "a", "b", "c" }, "a", null));
    // not matching
    Assert.assertFalse(RouterPathLookup.beginsWith(new String[]{ "a", "b", "c"}, "b", "c"));
    // should fail as actual has two extra string at end
    Assert.assertFalse(RouterPathLookup.beginsWith(new String[]{"a", "b", "c", "d"}, null, "c", "d"));
  }

  @Test
  public void testEndsWith() {
    // expected should not be longer than actual
    Assert.assertFalse(RouterPathLookup.endsWith(new String[]{"a", "b", "c"}, "a", "b", "c", "d"));
    // should pass as ends with is correct
    Assert.assertTrue(RouterPathLookup.endsWith(new String[]{"a", "b", "c"}, "b", "c"));
    // should fail as actual does not end with 'c'
    Assert.assertFalse(RouterPathLookup.endsWith(new String[]{"a", "b", "c"}, "a", "b"));
    // should pass as actual has one extra string at end
    Assert.assertTrue(RouterPathLookup.endsWith(new String[]{"a", "b", "c"}, "a", "b", null));
    // should fail as actual has two extra string at end
    Assert.assertFalse(RouterPathLookup.endsWith(new String[]{"a", "b", "c", "d"}, "a", "b", null));
  }

  private void assertRouting(String path, RouteDestination destination) {
    for (HttpMethod method : ImmutableList.of(HttpMethod.GET, HttpMethod.POST, HttpMethod.DELETE)) {
      HttpRequest httpRequest = new DefaultHttpRequest(VERSION, method, path);
      RouteDestination result = pathLookup.getRoutingService(path, httpRequest);
      Assert.assertEquals(destination,  result);
    }
  }

  private void assertContent(String path, HttpMethod method, @Nullable String expectedService) {
    RouteDestination expectedDestination = expectedService == null ? null : new RouteDestination(expectedService);
    long l = System.currentTimeMillis();
    RouteDestination auditLogContent = AUDIT_LOOK_UP.getAuditLogContent(path, method);
    System.out.println(System.currentTimeMillis() - l);
    System.out.println("###");
    Assert.assertEquals(expectedDestination, auditLogContent);
  }
}
