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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.auth.NoAuthenticator;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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
    pathLookup = new RouterPathLookup(new NoAuthenticator());
  }

  @Test
  public void testMetricsPath() throws Exception {
    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
    String flowPath = "/v2///metrics/system/apps/InvalidApp//";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);

    flowPath = "/v2/metrics";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);

    flowPath = "/v2/metrics//";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);
  }

  @Test
  public void testAppFabricPath() throws Exception {
    //Default destination for URIs will APP_FABRIC_HTTP
    String flowPath = "/v2/ping/";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    flowPath = "/status";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    flowPath = "/v2/monitor///abcd/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testLogPath() throws Exception {
    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
    String flowPath = "/v2/apps//InvalidApp///procedures/ProcName/logs/";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);

    flowPath = "///v2///apps/InvalidApp/flows/FlowName/////logs";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);

    flowPath = "v2/apps/InvalidApp/procedures/ProName/logs/abcd";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);
  }

  @Test
  public void testProcedurePath() throws Exception {
    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
    //Procedure Path /v2/apps/<appid>/procedures/<procedureid>/methods/<methodName>
    //Discoverable Service Name -> procedure.%s.%s.%s", namespaceId, appId, procedureName;
    //TODO: Add tests that test for a non-default namespaceId

    String flowPath = "/v2/apps//InvalidApp///procedures/ProcName///methods//H?user=asd";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals("procedure." + Constants.DEFAULT_NAMESPACE + ".InvalidApp.ProcName", result);

    flowPath = "///v2///apps/Invali_-123//procedures/Hel123@!@!//methods/Asdad?das////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals("procedure." + Constants.DEFAULT_NAMESPACE + ".Invali_-123.Hel123@!@!", result);

    flowPath = "v2/apps/InvalidApp/procedures/ProName/methods/getCustomer";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals("procedure." + Constants.DEFAULT_NAMESPACE + ".InvalidApp.ProName", result);

    flowPath = "v2/apps/InvalidApp/procedures/ProName/methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    flowPath = "v2/apps/InvalidApp/procedures/ProName/methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);
  }


  @Test
  public void testServicePath() throws Exception {
    // Service Path "/v2/apps/{app-id}/services/{service-id}/methods/<user-defined-method-path>"
    // Discoverable Service Name -> "service.%s.%s.%s", namespaceId, appId, serviceId
    //TODO: Add tests that test for a non-default namespaceId

    String servicePath = "/v2/apps//PurchaseHistory///services/CatalogLookup///methods//ping/1";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals("service." + Constants.DEFAULT_NAMESPACE + ".PurchaseHistory.CatalogLookup", result);

    servicePath = "///v2///apps/PurchaseHistory-123//services/weird!service@@NAme///methods/echo/someParam";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), servicePath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals("service." + Constants.DEFAULT_NAMESPACE + ".PurchaseHistory-123.weird!service@@NAme", result);

    servicePath = "v2/apps/SomeApp_Name/services/CatalogLookup/methods/getHistory/itemID";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals("service." + Constants.DEFAULT_NAMESPACE + ".SomeApp_Name.CatalogLookup", result);

    // The following two should resort to resort to APP_FABRIC_HTTP, because there is no actual method being called.
    // (Similar to how procedures methods are routed).
    servicePath = "v2/apps/AppName/services/CatalogLookup//methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), servicePath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    servicePath = "v2/apps/otherAppName/services/CatalogLookup//methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), servicePath);
    httpRequest.setHeader(Constants.Gateway.API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, servicePath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testStreamPath() throws Exception {
    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
    String flowPath = "/v2/streams";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    flowPath = "///v2/streams///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    flowPath = "v2///streams///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    flowPath = "//v2///streams/HelloStream//flows///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    flowPath = "//v2///streams/HelloStream//flows///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.STREAMS, result);

    flowPath = "//v2///streams/HelloStream//flows///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.STREAMS, result);

    flowPath = "v2//streams//flows///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.STREAMS, result);

    flowPath = "v2//streams/InvalidStreamName/flows/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    flowPath = "v2//streams/InvalidStreamName/flows/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.STREAMS, result);

    flowPath = "v2//streams/InvalidStreamName/info/";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.STREAMS, result);
  }

  @Test
  public void testRouterFlowPathLookUp() throws Exception {
    String flowPath = "/v2//apps/ResponseCodeAnalytics/flows/LogAnalyticsFlow/status";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testRouterWorkFlowPathLookUp() throws Exception {
    String procPath = "/v2/apps///PurchaseHistory///workflows/PurchaseHistoryWorkflow/status";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), procPath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, procPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterProcedurePathLookUp() throws Exception {
    String procPath = "/v2//apps/ResponseCodeAnalytics/procedures/StatusCodeProcedure/status";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), procPath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, procPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterDeployPathLookUp() throws Exception {
    String procPath = "/v2//apps/";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), procPath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, procPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterFlowletInstancesLookUp() throws Exception {
    String procPath = "/v2//apps/WordCount/flows/WordCountFlow/flowlets/StreamSource/instances";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), procPath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, procPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP,  result);
  }

  @Test
  public void testRouterExplorePathLookUp() throws Exception {
    String explorePath = "/v2//data///explore//datasets////mydataset//enable";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), explorePath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, explorePath, httpRequest);
    Assert.assertEquals(Constants.Service.EXPLORE_HTTP_USER_SERVICE, result);
  }

  @Test
  public void testRouterWebAppPathLookUp() throws Exception {
    //Calls to webapp service with appName in the first split of URI will be routed to webappService
    //But if it has v2 then use the regular router lookup logic to find the appropriate service
    final String webAppService = "webapp$HOST";
    String procPath = "/sentiApp/abcd/efgh///";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), procPath);
    String result = pathLookup.getRoutingService(webAppService, procPath, httpRequest);
    Assert.assertEquals(webAppService, result);

    procPath = "/v2//metrics///";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), procPath);
    result = pathLookup.getRoutingService(webAppService, procPath, httpRequest);
    Assert.assertEquals(Constants.Service.METRICS, result);
  }

  @Test
  public void testRouterV3PathLookup() {
    final String namespacePath = "/v3////namespace/////";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), namespacePath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, namespacePath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);
  }

  @Test
  public void testRouterFeedsLookup() {
    final String namespacePath = "/v3//feeds/test";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("PUT"), namespacePath);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, namespacePath, httpRequest);
    Assert.assertEquals(null, result);
  }
}
