package com.continuuity.security.server;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.security.auth.AccessToken;
import com.continuuity.security.auth.AccessTokenCodec;
import com.continuuity.security.guice.InMemorySecurityModule;
import com.continuuity.security.io.Codec;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Service;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class for ExternalAuthenticationServer.
 */
public class TestExternalAuthenticationServer {
  private static final Logger LOG = LoggerFactory.getLogger(TestExternalAuthenticationServer.class);
  private static ExternalAuthenticationServer server;
  private static CConfiguration configuration;
  private static int port;
  private static Codec<AccessToken> tokenCodec;
  private static DiscoveryServiceClient discoveryServiceClient;

  @BeforeClass
  public static void setup() {
    Injector injector = Guice.createInjector(new IOModule(), new InMemorySecurityModule(), new ConfigModule(),
                                             new DiscoveryRuntimeModule().getInMemoryModules());
    server = injector.getInstance(ExternalAuthenticationServer.class);
    configuration = injector.getInstance(CConfiguration.class);
    tokenCodec = injector.getInstance(AccessTokenCodec.class);
    discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    port = configuration.getInt(Constants.Security.AUTH_SERVER_PORT);
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
    String tokenType = responseJson.get(ExternalAuthenticationServer.ResponseFields.TOKEN_TYPE).toString();
    int expiration = responseJson.get(ExternalAuthenticationServer.ResponseFields.EXPIRES_IN).getAsInt();

    assertTrue(tokenType.equals(String.format("\"%s\"", ExternalAuthenticationServer.ResponseFields.TOKEN_TYPE_BODY)));

    long expectedExpiration =  configuration.getInt(Constants.Security.TOKEN_EXPIRATION);
    // Test expiration time in seconds
    assertTrue(expiration == expectedExpiration / 1000);

    // Test that the server passes back an AccessToken object which can be decoded correctly.
    String encodedToken = responseJson.get(ExternalAuthenticationServer.ResponseFields.ACCESS_TOKEN).getAsString();
    AccessToken token = tokenCodec.decode(Base64.decodeBase64(encodedToken));
    LOG.info("AccessToken got from ExternalAuthenticationServer is: " + Bytes.toStringBinary(tokenCodec.encode(token)));

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

  /**
   * Test that the service is discoverable.
   * @throws Exception
   */
  @Test
  public void testServiceRegistration() throws Exception {
    server.startAndWait();
    Iterable<Discoverable> discoverables = discoveryServiceClient.discover(Constants.Service.EXTERNAL_AUTHENTICATION);
    Iterator<Discoverable> discoverableIterator = discoverables.iterator();

    assertTrue(discoverableIterator.hasNext());
    Discoverable discoverable = discoverableIterator.next();
    assertEquals(discoverable.getSocketAddress(), server.getSocketAddress());

    server.stopAndWait();
  }
}
