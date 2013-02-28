package com.continuuity.passport.passportClient;

import com.continuuity.common.utils.PortDetector;
import com.continuuity.passport.http.client.AccountProvider;
import com.continuuity.passport.http.client.PassportClient;
import com.continuuity.passport.server.MockServer;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestPassportClient {

  private static MockServer server ;
  private static int port;

  @BeforeClass
  public static void startServer() throws Exception {
    port = PortDetector.findFreePort();
    server = new MockServer(port);
    System.out.println("Starting server");
    server.start();
    Thread.sleep(1000);
    assertTrue(server.isStarted());
    Map<String,String> accountHash=  new HashMap<String, String>();
    accountHash.put("email_id", "john@smith.com");
    accountHash.put("first_name", "john");
    accountHash.put("last_name", "smith");
    accountHash.put("company", "continuuity");

    addAccount(accountHash);
    System.out.println("Added account");
 }
  @Test
  public void testValidAccount() throws URISyntaxException {
    PassportClient client = PassportClient.create(String.format("http://localhost:%d",port));
    System.out.println("Trying to get account");
    AccountProvider accountProvider = client.getAccount("apiKey1");
    System.out.println("Got an account");

    assert (accountProvider !=null);
    System.out.println(accountProvider.getAccountId());
    assert ("1".equals(accountProvider.getAccountId()));
    System.out.println("Tested");
  }

  @Test(expected = RuntimeException.class)
  public void testInvalidAccount() throws URISyntaxException {
    PassportClient client = PassportClient.create(String.format("http://localhost:%d",port));
    AccountProvider accountProvider = client.getAccount("apiKey100");

  }

  @AfterClass
  public static void stopServer() throws Exception {
     server.stop();
     Thread.sleep(1000);
  }

  private static void addAccount(Map<String,String> account) throws IOException {
    Gson gson =new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();

    HttpPost post = new HttpPost(String.format("http://localhost:%d/passport/v1/account",port));
    StringEntity entity =  new StringEntity(gson.toJson(account));
    entity.setContentType("application/json");
    post.setEntity(entity);

    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(post);

    assert(response.getStatusLine().getStatusCode() == 200);
  }


}
