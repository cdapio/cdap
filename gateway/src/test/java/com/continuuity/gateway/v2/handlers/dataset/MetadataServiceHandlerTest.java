package com.continuuity.gateway.v2.handlers.dataset;

import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Application;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.metadata.thrift.Flow;
import com.continuuity.metadata.thrift.Mapreduce;
import com.continuuity.metadata.thrift.Query;
import com.continuuity.metadata.thrift.Stream;
import com.google.common.collect.ImmutableList;
import junit.framework.Assert;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests all the endpoints associated with metadataservice handler.
 */
public class MetadataServiceHandlerTest {
  private static String account = "developer";

  @Before
  public void setUp() throws Exception {

    Application app1 = new Application("app1");
    Stream s1 = new Stream("s1");
    Dataset d1 = new Dataset("d1");
    Flow f1 = new Flow("flow", "app1");
    f1.setDatasets(ImmutableList.of("d1"));
    f1.setStreams(ImmutableList.of("s1"));
    Query q1 = new Query("q1", "app1");
    q1.setDatasets(ImmutableList.of("d1"));
    Mapreduce mr1 = new Mapreduce("mr1", "app1");
    mr1.setDatasets(ImmutableList.of("d1"));

    app1.setName("app1-name");
    s1.setName("s1-name");
    d1.setName("d1-name");
    d1.setType("d1-type");
    q1.setName("q1-name");
    q1.setServiceName("q1-servicename");
    f1.setName("f1-name");
    mr1.setName("mr1-name");

    GatewayFastTestsSuite.getMds().createApplication(new Account(account), app1);
    GatewayFastTestsSuite.getMds().createStream(new Account(account), s1);
    GatewayFastTestsSuite.getMds().createDataset(new Account(account), d1);
    GatewayFastTestsSuite.getMds().createQuery(new Account(account), q1);
    GatewayFastTestsSuite.getMds().createFlow(account, f1);
    GatewayFastTestsSuite.getMds().createMapreduce(new Account(account), mr1);
  }

  @After
  public void destroy() throws Exception {
    GatewayFastTestsSuite.getMds().deleteAll(account);
  }

  @Test
  public void testGetStreams() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.GET("/rest/v2/streams");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(s);
  }

  @Test
  public void testGetStreamsByApp() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.GET("/rest/v2/apps/app1/streams");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(s);
  }

  @Test
  public void testGetInvalidStreamsByApp() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.GET("/rest/v2/apps/app2/streams");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testGetDatasets() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.GET("/rest/v2/datasets");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(s);
  }

  @Test
  public void testGetDatasetsByApp() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.GET("/rest/v2/apps/app1/datasets");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(s);
  }

  @Test
  public void testGetQueries() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.GET("/rest/v2/procedures");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(s);
  }

  @Test
  public void testGetQueriesByApp() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.GET("/rest/v2/apps/app1/procedures");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(s);
  }

  @Test
  public void testGetMapReduces() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.GET("/rest/v2/mapreduces");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(s);
  }

  @Test
  public void testGetMapReducesByApp() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.GET("/rest/v2/apps/app1/mapreduces");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(s);
  }

  @Test
  public void testGetApps() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.GET("/rest/v2/apps");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(s);
  }

  @Test
  public void testGetFlows() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.GET("/rest/v2/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(s);
  }

  @Test
  public void testGetFlowsByApp() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.GET("/rest/v2/apps/app1/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Assert.assertNotNull(s);
  }
}
