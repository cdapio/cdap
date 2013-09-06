package com.continuuity.gateway.v2.handlers.v2.appfabric;

import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

/**
 * Testing of App Fabric REST Endpoints.
 */
public class AppFabricServiceHandlerTest {

  @Test
  public void testDeploy() throws Exception {
    File archive = FileUtils.toFile(getClass().getResource("/WordCount.jar"));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      ByteStreams.copy(new FileInputStream(archive), bos);
    } finally {
      bos.close();
    }

    HttpPut put = GatewayFastTestsSuite.getPUT("/v2/apps");
    put.setHeader(GatewayAuthenticator.CONTINUUITY_API_KEY, "api-key-example");
    put.setHeader("X-Archive-Name", "WordCount.jar");
    put.setEntity(new ByteArrayEntity(bos.toByteArray()));
    HttpResponse response = GatewayFastTestsSuite.PUT(put);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testDeleteApp() throws Exception {

  }

  @Test
  public void testRunnableHistory() throws Exception {

  }

  @Test
  public void testGetFlowletInstances() throws Exception {

  }

  @Test
  public void testSetFlowletInstances() throws Exception {

  }

  @Test
  public void testRunnableStatus() throws Exception {

  }

  @Test
  public void testRunnableSpecification() throws Exception {

  }

  @Test
  public void testRunnableStart() throws Exception {

  }

  @Test
  public void testRunnableStop() throws Exception {

  }
}
