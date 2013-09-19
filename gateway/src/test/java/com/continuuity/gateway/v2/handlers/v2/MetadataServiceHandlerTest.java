package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Application;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.metadata.thrift.Flow;
import com.continuuity.metadata.thrift.Mapreduce;
import com.continuuity.metadata.thrift.Query;
import com.continuuity.metadata.thrift.Stream;
import com.continuuity.metadata.thrift.Workflow;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

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
    Flow f1 = new Flow("f1", "app1");
    f1.setDatasets(ImmutableList.of("d1"));
    f1.setStreams(ImmutableList.of("s1"));
    Query q1 = new Query("q1", "app1");
    q1.setDatasets(ImmutableList.of("d1"));
    Mapreduce mr1 = new Mapreduce("mr1", "app1");
    mr1.setDatasets(ImmutableList.of("d1"));
    Workflow workflow = new Workflow("wf1", "app1");

    app1.setName("app1-name");
    s1.setName("s1-name");
    s1.setDescription("s1-desc");
    d1.setName("d1-name");
    d1.setDescription("d1-desc");
    d1.setType("d1-type");
    q1.setName("q1-name");
    q1.setDescription("q1-desc");
    q1.setApplication("app1");
    q1.setServiceName("q1-servicename");
    f1.setName("f1-name");
    f1.setApplication("app1");
    mr1.setName("mr1-name");
    mr1.setDescription("mr1-desc");
    mr1.setApplication("app1");
    workflow.setName("wf1-name");
    workflow.setApplication("app1");

    GatewayFastTestsSuite.getMds().createApplication(new Account(account), app1);
    GatewayFastTestsSuite.getMds().createStream(new Account(account), s1);
    GatewayFastTestsSuite.getMds().createDataset(new Account(account), d1);
    GatewayFastTestsSuite.getMds().createQuery(new Account(account), q1);
    GatewayFastTestsSuite.getMds().createFlow(account, f1);
    GatewayFastTestsSuite.getMds().createMapreduce(new Account(account), mr1);
    GatewayFastTestsSuite.getMds().createWorkflow(account, workflow);
  }

  @After
  public void destroy() throws Exception {
    GatewayFastTestsSuite.getMds().deleteAll(account);
  }

  @Test
  public void testGetStreams() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/streams");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {
    }.getType());
    Assert.assertEquals("s1", o.get(0).get("id"));
    Assert.assertEquals("s1-name", o.get(0).get("name"));
    Assert.assertEquals("s1-desc", o.get(0).get("description"));
  }

  @Test
  public void testGetStreamsByApp() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/apps/app1/streams");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals("s1", o.get(0).get("id"));
    Assert.assertEquals("s1-name", o.get(0).get("name"));
    Assert.assertEquals("s1-desc", o.get(0).get("description"));
  }

  @Test
  public void testGetInvalidStreamsByApp() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/apps/app2/streams");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testGetDatasets() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/datasets");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals("d1", o.get(0).get("id"));
    Assert.assertEquals("d1-name", o.get(0).get("name"));
    Assert.assertEquals("d1-desc", o.get(0).get("description"));
  }

  @Test
  public void testGetDatasetsByApp() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/apps/app1/datasets");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals("d1", o.get(0).get("id"));
    Assert.assertEquals("d1-name", o.get(0).get("name"));
    Assert.assertEquals("d1-desc", o.get(0).get("description"));
  }

  @Test
  public void testGetQueries() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/procedures");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals("q1", o.get(0).get("id"));
    Assert.assertEquals("q1-name", o.get(0).get("name"));
    Assert.assertEquals("q1-desc", o.get(0).get("description"));
    Assert.assertEquals("app1", o.get(0).get("app"));
  }

  @Test
  public void testGetQueriesByApp() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/apps/app1/procedures");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals("q1", o.get(0).get("id"));
    Assert.assertEquals("q1-name", o.get(0).get("name"));
    Assert.assertEquals("q1-desc", o.get(0).get("description"));
    Assert.assertEquals("app1", o.get(0).get("app"));
  }

  @Test
  public void testGetMapReduces() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/mapreduces");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals("mr1", o.get(0).get("id"));
    Assert.assertEquals("mr1-name", o.get(0).get("name"));
    Assert.assertEquals("mr1-desc", o.get(0).get("description"));
    Assert.assertEquals("app1", o.get(0).get("app"));
  }

  @Test
  public void testGetMapReducesByApp() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/apps/app1/mapreduces");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals("mr1", o.get(0).get("id"));
    Assert.assertEquals("mr1-name", o.get(0).get("name"));
    Assert.assertEquals("mr1-desc", o.get(0).get("description"));
    Assert.assertEquals("app1", o.get(0).get("app"));
  }

  @Test
  public void testGetApps() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/apps");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals("app1", o.get(0).get("id"));
    Assert.assertEquals("app1-name", o.get(0).get("name"));
  }

  @Test
  public void testGetFlows() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals("f1", o.get(0).get("id"));
    Assert.assertEquals("f1-name", o.get(0).get("name"));
    Assert.assertEquals("app1", o.get(0).get("app"));
  }

  @Test
  public void testGetFlowsByApp() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/apps/app1/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals("f1", o.get(0).get("id"));
    Assert.assertEquals("f1-name", o.get(0).get("name"));
    Assert.assertEquals("app1", o.get(0).get("app"));
  }

  @Test
  public void testGetWorkflows() throws Exception{
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/workflows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals("wf1", o.get(0).get("id"));
    Assert.assertEquals("wf1-name", o.get(0).get("name"));
    Assert.assertEquals("app1", o.get(0).get("app"));
  }

  @Test
  public void testGetWorkflowsByApp() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doGet("/v2/apps/app1/workflows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>() {}.getType());
    Assert.assertEquals("wf1", o.get(0).get("id"));
    Assert.assertEquals("wf1-name", o.get(0).get("name"));
    Assert.assertEquals("app1", o.get(0).get("app"));
  }
}
