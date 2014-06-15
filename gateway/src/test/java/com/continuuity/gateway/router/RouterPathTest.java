package com.continuuity.gateway.router;

import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.auth.NoAuthenticator;
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
    String flowPath = "/v2///metrics/reactor/apps/InvalidApp//";
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
    //Discoverable Service Name -> procedure.%s.%s.%s", accountId, appId, procedureName;
    String accId = "developer";

    String flowPath = "/v2/apps//InvalidApp///procedures/ProcName///methods//H?user=asd";
    HttpRequest httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    httpRequest.setHeader(Constants.Gateway.CONTINUUITY_API_KEY, API_KEY);
    String result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals("procedure." + accId + ".InvalidApp.ProcName", result);

    flowPath = "///v2///apps/Invali_-123//procedures/Hel123@!@!//methods/Asdad?das////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
    httpRequest.setHeader(Constants.Gateway.CONTINUUITY_API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals("procedure." + accId + ".Invali_-123.Hel123@!@!", result);

    flowPath = "v2/apps/InvalidApp/procedures/ProName/methods/getCustomer";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    httpRequest.setHeader(Constants.Gateway.CONTINUUITY_API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals("procedure." + accId + ".InvalidApp.ProName", result);

    flowPath = "v2/apps/InvalidApp/procedures/ProName/methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), flowPath);
    httpRequest.setHeader(Constants.Gateway.CONTINUUITY_API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
    Assert.assertEquals(Constants.Service.APP_FABRIC_HTTP, result);

    flowPath = "v2/apps/InvalidApp/procedures/ProName/methods////";
    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), flowPath);
    httpRequest.setHeader(Constants.Gateway.CONTINUUITY_API_KEY, API_KEY);
    result = pathLookup.getRoutingService(FALLBACKSERVICE, flowPath, httpRequest);
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

}
