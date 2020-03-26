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

package io.cdap.cdap.gateway.router;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.proto.ProgramType;
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
public class RouterPathLookupTest {

  private static RouterPathLookup pathLookup;
  private static final HttpVersion VERSION = HttpVersion.HTTP_1_1;
  private static final String API_KEY = "SampleTestApiKey";

  @BeforeClass
  public static void init() {
    pathLookup = new RouterPathLookup();
  }

  @Test
  public void testBootstrapPath() {
    String path = "/v3/bootstrap";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), path);
    RouteDestination result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testBatchRunsPath() {
    String path = "/v3/namespaces/n1/runs";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), path);
    RouteDestination result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
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
  public void testSystemServicePath() {
    String path = "/v3/system/services/foo/logs";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    RouteDestination result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.LOG_QUERY, result);

    path = "/v3/system/services/foo/live-info";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testMetricsPath() {
    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
    String path = "/v3///metrics/system/apps/InvalidApp//";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    RouteDestination result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.METRICS, result);

    path = "/v3/metrics";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), path);
    result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.METRICS, result);

    path = "/v3/metrics//";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), path);
    result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.METRICS, result);

    testMetricsPath("/v3/metrics/search?target=tag&tag=namespace:user");
    testMetricsPath("/v3/metrics/search?target=tag&tag=app:PurchaeHistory&tag=service:PurchaseService");
    testMetricsPath("/v3/metrics/search?target=metric&tag=app:PurchaeHistory&tag=service:PurchaseService");
  }

  private void testMetricsPath(String path) {
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    RouteDestination result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.METRICS, result);
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
    String path = "/v3/namespaces/default/apps//InvalidApp///services/ServiceName/logs/";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    RouteDestination result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.LOG_QUERY, result);

    path = "///v3/namespaces/default///apps/InvalidApp/services/ServiceName/////logs";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), path);
    result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.LOG_QUERY, result);

    path = "/v3/namespaces/default/apps/InvalidApp/service/ServiceName/runs/7e6adc79-0f5d-4252-70817ea47698/logs/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.LOG_QUERY, result);
  }

  @Test
  public void testServicePath() {
    // The following two should resort to resort to APP_FABRIC_HTTP, because there is no actual method being called.
    String servicePath = "v3/namespaces/default/apps/AppName/services/CatalogLookup//methods////";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), servicePath);
    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
    RouteDestination result = pathLookup.getRoutingService(servicePath, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    servicePath = "v3/namespaces/some/apps/otherAppName/services/CatalogLookup//methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(servicePath, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    // v3 servicePaths
    servicePath = "/v3/namespaces/testnamespace/apps//PurchaseHistory///services/CatalogLookup///methods//ping/1";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(servicePath, httpRequest);
    Assert.assertEquals(String.format("%s.testnamespace.PurchaseHistory.CatalogLookup",
                                      ProgramType.SERVICE.getDiscoverableTypeName()),
                        result.getServiceName());
    Assert.assertNull(result.getVersion());

    servicePath = "///v3/namespaces/testnamespace//apps/PurchaseHistory-123//services/weird!service@@NAme///methods/" +
      "echo/someParam";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), servicePath);
    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(servicePath, httpRequest);
    Assert.assertEquals(String.format("%s.testnamespace.PurchaseHistory-123.weird!service@@NAme",
                                      ProgramType.SERVICE.getDiscoverableTypeName()),
                        result.getServiceName());
    Assert.assertNull(result.getVersion());

    servicePath = "v3/namespaces/testnamespace/apps/SomeApp_Name/services/CatalogLookup/methods/getHistory/itemID";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(servicePath, httpRequest);
    Assert.assertEquals(String.format("%s.testnamespace.SomeApp_Name.CatalogLookup",
                                      ProgramType.SERVICE.getDiscoverableTypeName()),
                        result.getServiceName());
    Assert.assertNull(result.getVersion());

    servicePath = "v3/namespaces/testnamespace/apps/AppName/services/CatalogLookup//methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), servicePath);
    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(servicePath, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

    servicePath = "v3/namespaces/testnamespace/apps/AppName/services/CatalogLookup////methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.headers().set(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(servicePath, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testOpsDashboardPath() {
    assertRouting("/v3/dashboard", RouterPathLookup.APP_FABRIC_HTTP);
  }

  @Test
  public void testRouterServicePathLookUp() {
    String path = "/v3/namespaces/default//apps/ResponseCodeAnalytics/services/LogAnalyticsService/status";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    RouteDestination result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testRouterWorkFlowPathLookUp() {
    String path = "/v3/namespaces/default/apps///PurchaseHistory///workflows/PurchaseHistoryWorkflow/status";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), path);
    RouteDestination result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterDeployPathLookUp() {
    String path = "/v3/namespaces/default//apps/";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), path);
    RouteDestination result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterServiceInstancesLookUp() {
    String path = "/v3/namespaces/default//apps/WordCount/services/WordCountService/instances";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), path);
    RouteDestination result = pathLookup.getRoutingService(path, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterExplorePathLookUp() {
    String explorePath = "/v3/namespaces/default//data///explore//datasets////mydataset//enable";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), explorePath);
    RouteDestination result = pathLookup.getRoutingService(explorePath, httpRequest);
    Assert.assertEquals(RouterPathLookup.EXPLORE_HTTP_USER_SERVICE, result);
  }

  @Test
  public void testRouterExploreStatusPathLookUp() {
    String explorePath = "/v3/explore/status";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), explorePath);
    RouteDestination result = pathLookup.getRoutingService(explorePath, httpRequest);
    Assert.assertEquals(RouterPathLookup.EXPLORE_HTTP_USER_SERVICE, result);
  }

  @Test
  public void testRouterV3PathLookup() {
    final String namespacePath = "/v3////namespace/////";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), namespacePath);
    RouteDestination result = pathLookup.getRoutingService(namespacePath, httpRequest);
    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testRouterFeedsLookup() {
    final String namespacePath = "/v3//feeds/test";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), namespacePath);
    RouteDestination result = pathLookup.getRoutingService(namespacePath, httpRequest);
    Assert.assertNull(result);
  }

  @Test
  public void testMetadataPath() {
    // all app metadata
    assertRouting("/v3/namespaces/default//apps/WordCount//////metadata", RouterPathLookup.METADATA_SERVICE);
    // all artifact metadata
    assertRouting("/v3/namespaces/default//artifacts/WordCount///versions/v1//metadata",
                  RouterPathLookup.METADATA_SERVICE);
    // all program metadata
    assertRouting("/v3/namespaces/default//apps/WordCount//services//ServiceName//metadata",
                  RouterPathLookup.METADATA_SERVICE);
    // all dataset metadata
    assertRouting("/v3/namespaces/default//datasets/ds1//////metadata", RouterPathLookup.METADATA_SERVICE);
    // app metadata properties
    assertRouting("/v3/namespaces/default//apps/WordCount//////metadata///////properties",
                  RouterPathLookup.METADATA_SERVICE);
    // artifact metadata properties
    assertRouting("/v3/namespaces/default//artifacts/WordCount///versions/v1//metadata/properties",
                  RouterPathLookup.METADATA_SERVICE);
    // program metadata properties
    assertRouting("/v3/namespaces/default//apps/WordCount/services/ServiceName/metadata/properties"
      , RouterPathLookup.METADATA_SERVICE);
    // dataset metadata properties
    assertRouting("/v3/namespaces/default/////datasets/ds1/metadata/properties", RouterPathLookup.METADATA_SERVICE);
    // app metadata tags
    assertRouting("/v3/namespaces/default//apps/WordCount/////metadata/tags", RouterPathLookup.METADATA_SERVICE);
    // artifact metadata tags
    assertRouting("/v3/namespaces/default//artifacts/WordCount//versions//1.0/metadata/tags",
                  RouterPathLookup.METADATA_SERVICE);
    // program metadata tags
    assertRouting("/v3/namespaces/default//apps/WordCount/services/ServiceName/metadata/tags",
                  RouterPathLookup.METADATA_SERVICE);
    // dataset metadata tags
    assertRouting("/v3/namespaces/default/////datasets/ds1/metadata/tags", RouterPathLookup.METADATA_SERVICE);
    // search metadata
    assertRouting("/v3/namespaces/default/metadata/search", RouterPathLookup.METADATA_SERVICE);
    assertRouting("/v3/metadata/search", RouterPathLookup.METADATA_SERVICE);
    // lineage
    assertRouting("/v3/namespaces/default/////datasets/ds1/lineage", RouterPathLookup.METADATA_SERVICE);
    // get metadata for accesses
    assertRouting("/v3/namespaces/default//apps/WordCount/services/ServiceName/runs/runid/metadata",
                  RouterPathLookup.METADATA_SERVICE);

    // test field lineage path
    assertRouting("/v3/namespaces/default/datasets/ds1/lineage/fields", RouterPathLookup.METADATA_SERVICE);
    assertRouting("/v3/namespaces/default/datasets/ds1/lineage/fields/field1", RouterPathLookup.METADATA_SERVICE);
    assertRouting("/v3/namespaces/default/datasets/ds1/lineage/fields/field1/operations",
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
  public void testMetadataInternalsPaths() {
    assertRouting("/v3/metadata-internals/create", RouterPathLookup.DONT_ROUTE);
    assertRouting("/v3/metadata-internals/drop", RouterPathLookup.DONT_ROUTE);
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

  @Test
  public void testProfilePaths() {
    assertRouting("v3/profiles", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("v3/profiles/p", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("v3/profiles/p/enable", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("v3/profiles/p/disable", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("v3/namespaces/default/profiles", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("v3/namespaces/default/profiles/p", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("v3/namespaces/default/profiles/p/enable", RouterPathLookup.APP_FABRIC_HTTP);
    assertRouting("v3/namespaces/default/profiles/p/disable", RouterPathLookup.APP_FABRIC_HTTP);
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
}
