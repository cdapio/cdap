package com.continuuity.passport.httpendpoints;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.passport.Constants;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.testhelper.HyperSQL;
import com.continuuity.passport.testhelper.TestPassportServer;
import com.google.common.io.ByteStreams;
import com.google.gson.JsonObject;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertTrue;

/**
 *
 */

public class TestAccountHandler {

  private static TestPassportServer server;
  private static int port;

  @BeforeClass
  public static void setup() throws Exception {

    port = PortDetector.findFreePort();

    //Startup HSQL instance
    HyperSQL.startHsqlDB();

    CConfiguration configuration = CConfiguration.create();
    configuration.setInt(Constants.CFG_SERVER_PORT, port);

    String connectionString = "jdbc:hsqldb:mem:test;hsqldb.default_table_type=cached;hsqldb.sql.enforce_size=false" +
      "&user=sa&zeroDateTimeBehavior=convertToNull";

    configuration.set(Constants.CFG_JDBC_CONNECTION_STRING, connectionString);
    String profanePath = TestAccountHandler.class.getResource("/ProfaneWords").getPath();

    configuration.set(Constants.CFG_PROFANE_WORDS_FILE_PATH, profanePath);
    server = new TestPassportServer(configuration);

    System.out.println("Starting server");
    server.start();
    Thread.sleep(1000);
    assertTrue(server.isStarted());
  }

  @AfterClass
  public static void teardown() throws Exception {
    server.stop();
    HyperSQL.stopHsqlDB();
  }

  @Test
  public void accountCreate() throws IOException {
    String endPoint = String.format("http://localhost:%d/passport/v1/account", port);
    HttpPost post = new HttpPost(endPoint);
    post.setEntity( new StringEntity( getJson("sree@continuuity.com")));
    post.addHeader("Content-Type", "application/json");

    String result = request(post);
    assertTrue(result != null);
    Account account =  Account.fromString(result);
    assertTrue("sree@continuuity.com".equals(account.getEmailId()));
  }


  @Test
  public void testAccounts() throws IOException {
    String endPoint = String.format("http://localhost:%d/passport/v1/account", port);
    HttpPost post = new HttpPost(endPoint);
    post.setEntity( new StringEntity( getJson("john.smith@continuuity.com")));
    post.addHeader("Content-Type", "application/json");

    String result = request(post);
    assertTrue(result != null);
    Account account =  Account.fromString(result);
    assertTrue("john.smith@continuuity.com".equals(account.getEmailId()));
    int id = account.getAccountId();

    endPoint  = String.format("http://localhost:%d/passport/v1/account/%d/confirmed", port,id);
    HttpPut put = new HttpPut(endPoint);
    put.setEntity(new StringEntity(getJson("john.smith@continuuity.com","john","smith")));
    put.setHeader("Content-Type","application/json");
    result = request(put);

    account = Account.fromString(result);
    assertTrue("john.smith@continuuity.com".equals(account.getEmailId()));
    assertTrue("john".equals(account.getFirstName()));
    assertTrue("smith".equals(account.getLastName()));

    endPoint  = String.format("http://localhost:%d/passport/v1/account/%d/paid", port,id);
    put = new HttpPut(endPoint);
    result = request(put);
    account = Account.fromString(result);
    assertTrue("john.smith@continuuity.com".equals(account.getEmailId()));

  }

  private String getJson(String emailId){
    JsonObject object = new JsonObject();
    object.addProperty("email_id", emailId);
    return object.toString();
  }

  private String getJson(String emailId, String firstName, String lastName){
    JsonObject object = new JsonObject();
    object.addProperty("email_id", emailId);
    object.addProperty("first_name", firstName);
    object.addProperty("last_name", lastName);
    object.addProperty("password", "123");
    object.addProperty("company", "foo");

    return object.toString();

  }


  public static String request(HttpUriRequest uri) throws IOException {
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(uri);
    assertTrue(response.getStatusLine().getStatusCode() == 200);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteStreams.copy(response.getEntity().getContent(), bos);
    String result = bos.toString("UTF-8");
    bos.close();
    return  result;
  }

}

