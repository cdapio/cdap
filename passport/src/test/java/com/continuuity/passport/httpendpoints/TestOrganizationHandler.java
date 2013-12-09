package com.continuuity.passport.httpendpoints;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.passport.Constants;
import com.continuuity.passport.meta.Organization;
import com.continuuity.passport.testhelper.HyperSQL;
import com.continuuity.passport.testhelper.TestPassportServer;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test Organization Handlers.
 */
public class TestOrganizationHandler {

  private static TestPassportServer server;
  private static int port;

  @BeforeClass
  public static void setup() throws Exception {

    //Startup HSQL instance
    HyperSQL.startHsqlDB();

    port = PortDetector.findFreePort();
    CConfiguration configuration = CConfiguration.create();
    configuration.setInt(Constants.CFG_SERVER_PORT, port);

    String connectionString = "jdbc:hsqldb:mem:test;hsqldb.default_table_type=cached;hsqldb.write_delay=false;" +
      "hsqldb.sql.enforce_size=false&user=sa&zeroDateTimeBehavior=convertToNull&autocommit=true";

    configuration.set(Constants.CFG_JDBC_CONNECTION_STRING, connectionString);
    String profanePath = TestAccountHandler.class.getResource("/ProfaneWords").getPath();

    configuration.set(Constants.CFG_PROFANE_WORDS_FILE_PATH, profanePath);
    server = new TestPassportServer(configuration);
    server.start();
  }

  @AfterClass
  public static void teardown() throws Exception {
    server.stop();
    HyperSQL.stopHsqlDB();
  }

  @Test
  public void orgCreate() throws IOException {
    String endPoint = String.format("http://localhost:%d/passport/v1/organizations", port);
    HttpPost post = new HttpPost(endPoint);
    post.setEntity(new StringEntity(TestPassportServer.getCompany("C123", "Continuuity")));
    post.addHeader("Content-Type", "application/json");

    String result = TestPassportServer.request(post);
    assertTrue(result != null);
    Organization org =  Organization.fromString(result);
    assertTrue("C123".equals(org.getId()));
    assertTrue("Continuuity".equals(org.getName()));

    //Try create again. Should give HttpStatus conflict
    HttpClient client = new DefaultHttpClient();
    HttpResponse response =  client.execute(post);
    assertEquals(409, response.getStatusLine().getStatusCode());

  }

  @Test
  public void orgDelete() throws IOException {
    //Create Org and delete later.
    String endPoint = String.format("http://localhost:%d/passport/v1/organizations", port);
    HttpPost post = new HttpPost(endPoint);
    post.setEntity(new StringEntity(TestPassportServer.getCompany("G123", "Google")));
    post.addHeader("Content-Type", "application/json");

    String result = TestPassportServer.request(post);
    assertTrue(result != null);
    Organization org =  Organization.fromString(result);
    assertTrue("G123".equals(org.getId()));
    assertTrue("Google".equals(org.getName()));

    endPoint = String.format("http://localhost:%d/passport/v1/organizations/%s", port, "G123");
    HttpDelete delete = new HttpDelete(endPoint);
    result = TestPassportServer.request(delete);
    assertTrue(result != null);

    //Try deleting a already deleted org. Should return 404.
    HttpClient client = new DefaultHttpClient();
    HttpResponse response =  client.execute(delete);
    assertEquals(404, response.getStatusLine().getStatusCode());
  }


  @Test
  public void orgGetAndPut() throws IOException {
    //Create Org
    String endPoint = String.format("http://localhost:%d/passport/v1/organizations", port);
    HttpPost post = new HttpPost(endPoint);
    post.setEntity(new StringEntity(TestPassportServer.getCompany("F123", "Facebok")));
    post.addHeader("Content-Type", "application/json");

    String result = TestPassportServer.request(post);
    assertTrue(result != null);
    Organization org =  Organization.fromString(result);
    assertTrue("F123".equals(org.getId()));
    assertTrue("Facebok".equals(org.getName()));

    endPoint = String.format("http://localhost:%d/passport/v1/organizations/%s", port, "F123");
    HttpPut put = new HttpPut(endPoint);
    put.setEntity(new StringEntity("{\"name\":\"Facebook\"}"));
    result = TestPassportServer.request(put);

    assertTrue(result != null);
    org =  Organization.fromString(result);
    assertTrue("F123".equals(org.getId()));
    assertTrue("Facebook".equals(org.getName()));

    //PUT non-existing.
    endPoint = String.format("http://localhost:%d/passport/v1/organizations/%s", port, "Z123");
    put = new HttpPut(endPoint);
    put.setEntity(new StringEntity("{\"name\":\"Zynga\"}"));
    HttpClient client = new DefaultHttpClient();
    HttpResponse response =  client.execute(put);
    assertEquals(404, response.getStatusLine().getStatusCode());


    endPoint = String.format("http://localhost:%d/passport/v1/organizations/%s", port, "F123");
    HttpGet get = new HttpGet(endPoint);
    result = TestPassportServer.request(get);
    org = Organization.fromString(result);
    assertTrue("F123".equals(org.getId()));
    assertTrue("Facebook".equals(org.getName()));
  }

  @Test
  public void getInvalid() throws IOException {
    //Get non existing org and check for 404
    String endPoint = String.format("http://localhost:%d/passport/v1/organizations/%s", port, "B123");
    HttpGet get = new HttpGet(endPoint);
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(get);
    assertEquals(404, response.getStatusLine().getStatusCode());
  }
}
