package com.continuuity.security.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.io.Codec;
import com.continuuity.common.utils.Networks;
import com.continuuity.security.auth.AccessToken;
import com.continuuity.security.auth.AccessTokenCodec;
import com.continuuity.security.guice.InMemorySecurityModule;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Service;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import com.unboundid.ldap.sdk.Entry;
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
import java.net.InetAddress;
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
  private static InMemoryDirectoryServer ldapServer;
  private static int ldapPort = Networks.getRandomPort();

  @BeforeClass
  public static void setup() {
    Injector injector = Guice.createInjector(new IOModule(), new InMemorySecurityModule(),
                                             new ConfigModule(getConfiguration()),
                                             new DiscoveryRuntimeModule().getInMemoryModules());
    server = injector.getInstance(ExternalAuthenticationServer.class);
    configuration = injector.getInstance(CConfiguration.class);
    tokenCodec = injector.getInstance(AccessTokenCodec.class);
    discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    port = configuration.getInt(Constants.Security.AUTH_SERVER_PORT);
    try {
      startLDAPServer();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * LDAP server and related handler configurations.
   * @return
   */
  private static CConfiguration getConfiguration() {
    String configBase = Constants.Security.AUTH_HANDLER_CONFIG_BASE;
    CConfiguration cConf = CConfiguration.create();
    // Use random port for testing
    cConf.setInt(Constants.Security.AUTH_SERVER_PORT, Networks.getRandomPort());
    cConf.set(Constants.Security.AUTH_HANDLER_CLASS,
              "com.continuuity.security.server.LDAPAuthenticationHandler");
    cConf.set(Constants.Security.LOGIN_MODULE_CLASS_NAME, "org.eclipse.jetty.plus.jaas.spi.LdapLoginModule");
    cConf.set(configBase.concat("debug"), "true");
    cConf.set(configBase.concat("hostname"), "localhost");
    cConf.set(configBase.concat("port"), Integer.toString(ldapPort));
    cConf.set(configBase.concat("userBaseDn"), "dc=example,dc=com");
    cConf.set(configBase.concat("userRdnAttribute"), "cn");
    cConf.set(configBase.concat("userObjectClass"), "inetorgperson");
    cConf.set(Constants.Security.SSL_ENABLED, "false");
    return cConf;
  }

  private static void startLDAPServer() throws Exception {
    InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig("dc=example,dc=com");
    config.setListenerConfigs(InMemoryListenerConfig.createLDAPConfig("LDAP", InetAddress.getByName("127.0.0.1"),
                                                                      ldapPort, null));
    Entry defaultEntry = new Entry(
                              "dn: dc=example,dc=com",
                              "objectClass: top",
                              "objectClass: domain",
                              "dc: example");
    Entry userEntry = new Entry(
                              "dn: uid=user,dc=example,dc=com",
                              "objectClass: inetorgperson",
                              "cn: admin",
                              "sn: User",
                              "uid: user",
                              "userPassword: realtime");
    ldapServer = new InMemoryDirectoryServer(config);
    ldapServer.addEntries(defaultEntry, userEntry);
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
    ldapServer.startListening();
    TimeUnit.SECONDS.sleep(3);
    HttpClient client = new DefaultHttpClient();
    String uri = String.format("http://localhost:%d/%s", port, GrantAccessToken.Paths.GET_TOKEN);
    HttpGet request = new HttpGet(uri);
    request.addHeader("Authorization", "Basic YWRtaW46cmVhbHRpbWU=");
    HttpResponse response = client.execute(request);

    assertEquals(response.getStatusLine().getStatusCode(), 200);

    // Test correct headers being returned
    String cacheControlHeader = response.getFirstHeader("Cache-Control").getValue();
    String pragmaHeader = response.getFirstHeader("Pragma").getValue();
    String contentType = response.getFirstHeader("Content-Type").getValue();

    assertEquals(cacheControlHeader, "no-store");
    assertEquals(pragmaHeader, "no-cache");
    assertEquals(contentType, "application/json;charset=UTF-8");

    // Test correct response body
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteStreams.copy(response.getEntity().getContent(), bos);
    String responseBody = bos.toString("UTF-8");
    bos.close();

    JsonParser parser = new JsonParser();
    JsonObject responseJson = (JsonObject) parser.parse(responseBody);
    String tokenType = responseJson.get(ExternalAuthenticationServer.ResponseFields.TOKEN_TYPE).toString();
    int expiration = responseJson.get(ExternalAuthenticationServer.ResponseFields.EXPIRES_IN).getAsInt();

    assertEquals(tokenType, String.format("\"%s\"", ExternalAuthenticationServer.ResponseFields.TOKEN_TYPE_BODY));

    long expectedExpiration =  configuration.getInt(Constants.Security.TOKEN_EXPIRATION);
    // Test expiration time in seconds
    assertEquals(expiration, expectedExpiration / 1000);

    // Test that the server passes back an AccessToken object which can be decoded correctly.
    String encodedToken = responseJson.get(ExternalAuthenticationServer.ResponseFields.ACCESS_TOKEN).getAsString();
    AccessToken token = tokenCodec.decode(Base64.decodeBase64(encodedToken));
    assertEquals(token.getIdentifier().getUsername(), "admin");
    LOG.info("AccessToken got from ExternalAuthenticationServer is: " + encodedToken);

    server.stopAndWait();
    ldapServer.shutDown(true);
  }

  /**
   * Test an unauthorized request to server.
   * @throws Exception
   */
  @Test
  public void testInvalidAuthentication() throws Exception {
    server.startAndWait();
    ldapServer.startListening();
    TimeUnit.SECONDS.sleep(3);
    HttpClient client = new DefaultHttpClient();
    String uri = String.format("http://localhost:%d/%s", port, GrantAccessToken.Paths.GET_TOKEN);
    HttpGet request = new HttpGet(uri);
    request.addHeader("Authorization", "xxxxx");

    HttpResponse response = client.execute(request);

    // Request is Unauthorized
    assertEquals(response.getStatusLine().getStatusCode(), 401);

    server.stopAndWait();
    ldapServer.shutDown(true);
  }

  /**
   * Test getting a long lasting Access Token.
   * @throws Exception
   */
  @Test
  public void testExtendedToken() throws Exception {
    server.startAndWait();
    ldapServer.startListening();
    TimeUnit.SECONDS.sleep(3);
    HttpClient client = new DefaultHttpClient();
    String uri = String.format("http://localhost:%d/%s", port, GrantAccessToken.Paths.GET_EXTENDED_TOKEN);
    HttpGet request = new HttpGet(uri);
    request.addHeader("Authorization", "Basic YWRtaW46cmVhbHRpbWU=");
    HttpResponse response = client.execute(request);

    assertEquals(response.getStatusLine().getStatusCode(), 200);

    // Test correct response body
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteStreams.copy(response.getEntity().getContent(), bos);
    String responseBody = bos.toString("UTF-8");
    bos.close();

    JsonParser parser = new JsonParser();
    JsonObject responseJson = (JsonObject) parser.parse(responseBody);
    int expiration = responseJson.get(ExternalAuthenticationServer.ResponseFields.EXPIRES_IN).getAsInt();

    long expectedExpiration =  configuration.getInt(Constants.Security.EXTENDED_TOKEN_EXPIRATION);
    // Test expiration time in seconds
    assertEquals(expiration, expectedExpiration / 1000);

    // Test that the server passes back an AccessToken object which can be decoded correctly.
    String encodedToken = responseJson.get(ExternalAuthenticationServer.ResponseFields.ACCESS_TOKEN).getAsString();
    AccessToken token = tokenCodec.decode(Base64.decodeBase64(encodedToken));
    assertEquals(token.getIdentifier().getUsername(), "admin");
    LOG.info("AccessToken got from ExternalAuthenticationServer is: " + encodedToken);

    server.stopAndWait();
    ldapServer.shutDown(true);
  }

  /**
   * Test that invalid paths return a 404 Not Found.
   * @throws Exception
   */
  @Test
  public void testInvalidPath() throws Exception {
    server.startAndWait();
    ldapServer.startListening();
    TimeUnit.SECONDS.sleep(3);
    HttpClient client = new DefaultHttpClient();
    String uri = String.format("http://localhost:%d/%s", port, "invalid");
    HttpGet request = new HttpGet(uri);
    request.addHeader("Authorization", "Basic YWRtaW46cmVhbHRpbWU=");
    HttpResponse response = client.execute(request);

    assertEquals(response.getStatusLine().getStatusCode(), 404);
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
