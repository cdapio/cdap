package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.security.Constants;
import com.continuuity.security.guice.SecurityModule;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Service;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Test class for ExternalAuthenticationServer.
 */
public class TestExternalAuthenticationServer {
  private static ExternalAuthenticationServer server;
  private static CConfiguration configuration;
  private static int port;

  @BeforeClass
  public static void setup() {
    Injector injector = Guice.createInjector(new IOModule(), new SecurityModule(), new ConfigModule());
    server = injector.getInstance(ExternalAuthenticationServer.class);
    configuration = CConfiguration.create();
    port = configuration.getInt(Constants.AUTH_SERVER_PORT, Constants.DEFAULT_AUTH_SERVER_PORT);
  }

  /**
   * Test that server starts and stops correctly.
   * @throws Exception
   */
  @Test
  public void testStartStopServer() throws Exception {
    Service.State state = server.startAndWait();
    Assert.assertTrue(state == Service.State.RUNNING);
    TimeUnit.SECONDS.sleep(5);
    state = server.stopAndWait();
    assertTrue(state == Service.State.TERMINATED);
  }

  /**
   * Test an authorized request to server.
   * @throws Exception
   */
  @Test
  public void testValidAuthentication() throws Exception {
    server.startAndWait();
    HttpClient client = new DefaultHttpClient();
    String uri = String.format("http://localhost:%d/", port);
    HttpGet request = new HttpGet(uri);
    request.addHeader("Authorization", "Basic YWRtaW46cmVhbHRpbWU=");
    HttpResponse response = client.execute(request);

    assertTrue(response.getStatusLine().getStatusCode() == 200);

    // Test correct headers being returned
    String cacheControlHeader = response.getFirstHeader("Cache-Control").getValue();
    String pragmaHeader = response.getFirstHeader("Pragma").getValue();
    String contentType = response.getFirstHeader("Content-Type").getValue();

    assertTrue((cacheControlHeader.equals("no-store")));
    assertTrue((pragmaHeader.equals("no-cache")));
    assertTrue((contentType.equals("application/json;charset=UTF-8")));

    // Test correct response body
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteStreams.copy(response.getEntity().getContent(), bos);
    String responseBody = bos.toString("UTF-8");
    bos.close();

    JsonParser parser = new JsonParser();
    JsonObject responseJson = (JsonObject) parser.parse(responseBody);
    String tokenType = responseJson.get("token_type").toString();
    int expiration = responseJson.get("expires_in").getAsInt();
    assertTrue(tokenType.equals("\"Bearer\""));
    long expectedExpiration =  configuration.getInt(Constants.TOKEN_EXPIRATION, Constants.DEFAULT_TOKEN_EXPIRATION);
    // Test expiration time in seconds
    assertTrue(expiration == expectedExpiration / 1000);

    server.stopAndWait();
  }

  /**
   * Test an unauthorized request to server.
   * @throws Exception
   */
  @Test
  public void testInvalidAuthentication() throws Exception {
    server.startAndWait();
    HttpClient client = new DefaultHttpClient();
    String uri = String.format("http://localhost:%d/", port);
    HttpGet request = new HttpGet(uri);
    request.addHeader("Authorization", "xxxxx");
    HttpResponse response = client.execute(request);

    // Request is Unauthorized
    assertTrue(response.getStatusLine().getStatusCode() == 401);

    server.stopAndWait();
  }
}
