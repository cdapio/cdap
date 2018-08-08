package co.cask.cdap.gateway.router;

import co.cask.cdap.common.conf.Constants;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import org.junit.Assert;
import org.junit.Test;

public class RouterPathFinderTest {
  private static final RouterPathFinder AUDIT_LOOK_UP = RouterPathFinder.getInstance();

  @Test
  public void testNamespaceEndpoints() throws Exception {
    // endpoints from DatasetInstanceHandler
    assertContent("/v3/namespaces/", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces/ns1", HttpMethod.GET, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces/ns1/properties", HttpMethod.PUT, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/namespaces/ns1", HttpMethod.PUT, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/unrecoverable/namespaces/ns1", HttpMethod.DELETE, Constants.Service.APP_FABRIC_HTTP);
    assertContent("/v3/unrecoverable/namespaces/ns1/datasets", HttpMethod.DELETE, Constants.Service.APP_FABRIC_HTTP);
  }


//  @Test
//  public void testMetricsPath() throws Exception {
//    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
////    String flowPath = "/v3/metrics/system/apps/InvalidApp/";
////    assertContent(flowPath, HttpMethod.GET, Constants.Service.METRICS);
//
////    String flowPath = "/v3/metrics";
////    assertContent(flowPath, HttpMethod.POST, Constants.Service.METRICS);
//
//    testMetricsPath("/v3/metrics/search?target=tag&tag=namespace:user");
//    testMetricsPath("/v3/metrics/search?target=tag&tag=app:PurchaeHistory&tag=flow:PurchaseFlow");
//    testMetricsPath("/v3/metrics/search?target=metric&tag=app:PurchaeHistory&tag=flow:PurchaseFlow");
//  }

  @Test
  public void testStreamPath() throws Exception {
    //Following URIs might not give actual results but we want to test resilience of Router Path Lookup
    String streamPath = "/v3/namespaces/default/streams";
    assertContent(streamPath, HttpMethod.GET, Constants.Service.STREAMS);

    streamPath = "/v3/namespaces/default/streams/s1";
    assertContent(streamPath, HttpMethod.GET, Constants.Service.STREAMS);
    assertContent(streamPath, HttpMethod.PUT, Constants.Service.STREAMS);
    assertContent(streamPath, HttpMethod.POST, Constants.Service.STREAMS);
    assertContent(streamPath, HttpMethod.DELETE, Constants.Service.STREAMS);
    assertContent(streamPath + "/async", HttpMethod.POST, Constants.Service.STREAMS);
    assertContent(streamPath + "/batch", HttpMethod.POST, Constants.Service.STREAMS);
    assertContent(streamPath + "/truncate", HttpMethod.POST, Constants.Service.STREAMS);
    assertContent(streamPath + "/properties", HttpMethod.PUT, Constants.Service.STREAMS);

//    assertContent(streamPath, HttpMethod.POST, Constants.Service.STREAMS);
//    assertContent(streamPath, HttpMethod.PUT, Constants.Service.STREAMS);



//    streamPath = "//v3/namespaces/default///streams/HelloStream//programs///";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
//    result = pathLookup.getRoutingService(streamPath, httpRequest);
//    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);

//    streamPath = "//v3/namespaces/default///streams/HelloStream//programs///";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), streamPath);
//    result = pathLookup.getRoutingService(streamPath, httpRequest);
//    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);
//
//    streamPath = "//v3/namespaces/default///streams/HelloStream//programs///";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("POST"), streamPath);
//    result = pathLookup.getRoutingService(streamPath, httpRequest);
//    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);
//
//    streamPath = "v3/namespaces/default//streams//flows///";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), streamPath);
//    result = pathLookup.getRoutingService(streamPath, httpRequest);
//    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);
//
//    streamPath = "v3/namespaces/default//streams/InvalidStreamName/programs/";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
//    result = pathLookup.getRoutingService(streamPath, httpRequest);
//    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
//
//    streamPath = "v3/namespaces/default//streams/InvalidStreamName/programs";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
//    result = pathLookup.getRoutingService(streamPath, httpRequest);
//    Assert.assertEquals(RouterPathLookup.APP_FABRIC_HTTP, result);
//
//    streamPath = "v3/namespaces/default//streams/InvalidStreamName/programs/";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("DELETE"), streamPath);
//    result = pathLookup.getRoutingService(streamPath, httpRequest);
//    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);
//
//    streamPath = "v3/namespaces/default//streams/InvalidStreamName/info/";
//    httpRequest = new DefaultHttpRequest(VERSION, new HttpMethod("GET"), streamPath);
//    result = pathLookup.getRoutingService(streamPath, httpRequest);
//    Assert.assertEquals(RouterPathLookup.STREAMS_SERVICE, result);
  }

  @Test
  public void testMetadataPath() throws Exception {
    assertContent("/v3/namespaces/default/apps/WordCount/metadata", HttpMethod.GET, Constants.Service.METADATA_SERVICE);
  }

  private void testMetricsPath(String path) throws Exception {
    assertContent(path, HttpMethod.POST, Constants.Service.METRICS);
  }

  private void assertContent(String path, HttpMethod method, String expectedService) throws Exception {
    Assert.assertEquals(new RouteDestination(expectedService), AUDIT_LOOK_UP.getAuditLogContent(path, method));
  }
}