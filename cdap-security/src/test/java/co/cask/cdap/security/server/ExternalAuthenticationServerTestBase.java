/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.security.server;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.io.Codec;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.security.auth.AccessToken;
import co.cask.cdap.security.auth.AccessTokenCodec;
import co.cask.cdap.security.guice.InMemorySecurityModule;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import com.unboundid.ldap.sdk.Entry;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.net.SocketAddress;
import java.net.URL;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.security.auth.login.Configuration;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Base test class for ExternalAuthenticationServer.
 */
public abstract class ExternalAuthenticationServerTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalAuthenticationServerTestBase.class);
  private static ExternalAuthenticationServer server;
  private static int port;
  private static Codec<AccessToken> tokenCodec;
  private static DiscoveryServiceClient discoveryServiceClient;
  private static InMemoryDirectoryServer ldapServer;
  protected static int ldapPort = Networks.getRandomPort();

  private static final Logger TEST_AUDIT_LOGGER = mock(Logger.class);

  // Needs to be set by derived classes.
  protected static CConfiguration configuration;
  protected static SConfiguration sConfiguration;
  protected static InMemoryListenerConfig ldapListenerConfig;

  protected abstract String getProtocol();
  protected abstract HttpClient getHTTPClient() throws Exception;

  protected static void setup() throws Exception {
    Assert.assertNotNull("CConfiguration needs to be set by derived classes", configuration);

    Module securityModule = Modules.override(new InMemorySecurityModule()).with(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(AuditLogHandler.class)
            .annotatedWith(Names.named(
              ExternalAuthenticationServer.NAMED_EXTERNAL_AUTH))
            .toInstance(new AuditLogHandler(TEST_AUDIT_LOGGER));
        }
      }
    );
    Injector injector = Guice.createInjector(new IOModule(), securityModule,
                                             new ConfigModule(getConfiguration(configuration),
                                                              HBaseConfiguration.create(), sConfiguration),
                                             new DiscoveryRuntimeModule().getInMemoryModules());
    server = injector.getInstance(ExternalAuthenticationServer.class);
    tokenCodec = injector.getInstance(AccessTokenCodec.class);
    discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);

    if (configuration.getBoolean(Constants.Security.SSL_ENABLED)) {
      port = configuration.getInt(Constants.Security.AuthenticationServer.SSL_PORT);
    } else {
      port = configuration.getInt(Constants.Security.AUTH_SERVER_BIND_PORT);
    }

    try {
      startLDAPServer();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    server.startAndWait();
    LOG.info("Auth server running on port {}", port);
    ldapServer.startListening();
    TimeUnit.SECONDS.sleep(3);
  }

  public int getAuthServerPort() {
    return port;
  }

  /**
   * LDAP server and related handler configurations.
   */
  private static CConfiguration getConfiguration(CConfiguration cConf) {
    String configBase = Constants.Security.AUTH_HANDLER_CONFIG_BASE;

    // Use random port for testing
    cConf.setInt(Constants.Security.AUTH_SERVER_BIND_PORT, Networks.getRandomPort());
    cConf.setInt(Constants.Security.AuthenticationServer.SSL_PORT, Networks.getRandomPort());

    cConf.set(Constants.Security.AUTH_HANDLER_CLASS, LDAPAuthenticationHandler.class.getName());
    cConf.set(Constants.Security.LOGIN_MODULE_CLASS_NAME, LDAPLoginModule.class.getName());
    cConf.set(configBase.concat("debug"), "true");
    cConf.set(configBase.concat("hostname"), "localhost");
    cConf.set(configBase.concat("port"), Integer.toString(ldapPort));
    cConf.set(configBase.concat("userBaseDn"), "dc=example,dc=com");
    cConf.set(configBase.concat("userRdnAttribute"), "cn");
    cConf.set(configBase.concat("userObjectClass"), "inetorgperson");

    URL keytabUrl = ExternalAuthenticationServerTestBase.class.getClassLoader().getResource("test.keytab");
    Assert.assertNotNull(keytabUrl);
    cConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH, keytabUrl.getPath());
    cConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL, "test_principal");
    return cConf;
  }

  private static void startLDAPServer() throws Exception {
    InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig("dc=example,dc=com");
    config.setListenerConfigs(ldapListenerConfig);

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

  @AfterClass
  public static void afterClass() throws Exception {
    ldapServer.shutDown(true);
    server.stopAndWait();
    // Clear any security properties for zookeeper.
    System.clearProperty(Constants.External.Zookeeper.ENV_AUTH_PROVIDER_1);
    Configuration.setConfiguration(null);
  }

  /**
   * Test an authorized request to server.
   * @throws Exception
   */
  @Test
  public void testValidAuthentication() throws Exception {
    HttpClient client = getHTTPClient();
    String uri = String.format("%s://%s:%d/%s", getProtocol(), server.getSocketAddress().getAddress().getHostAddress(),
                               server.getSocketAddress().getPort(), GrantAccessToken.Paths.GET_TOKEN);

    HttpGet request = new HttpGet(uri);
    request.addHeader("Authorization", "Basic YWRtaW46cmVhbHRpbWU=");
    HttpResponse response = client.execute(request);

    assertEquals(response.getStatusLine().getStatusCode(), 200);
    verify(TEST_AUDIT_LOGGER, timeout(10000).atLeastOnce()).trace(contains("admin"));

    // Test correct headers being returned
    String cacheControlHeader = response.getFirstHeader("Cache-Control").getValue();
    String pragmaHeader = response.getFirstHeader("Pragma").getValue();
    String contentType = response.getFirstHeader("Content-Type").getValue();

    assertEquals("no-store", cacheControlHeader);
    assertEquals("no-cache", pragmaHeader);
    assertEquals("application/json;charset=UTF-8", contentType);

    // Test correct response body
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteStreams.copy(response.getEntity().getContent(), bos);
    String responseBody = bos.toString("UTF-8");
    bos.close();

    JsonParser parser = new JsonParser();
    JsonObject responseJson = (JsonObject) parser.parse(responseBody);
    String tokenType = responseJson.get(ExternalAuthenticationServer.ResponseFields.TOKEN_TYPE).toString();
    long expiration = responseJson.get(ExternalAuthenticationServer.ResponseFields.EXPIRES_IN).getAsLong();

    assertEquals(String.format("\"%s\"", ExternalAuthenticationServer.ResponseFields.TOKEN_TYPE_BODY), tokenType);

    long expectedExpiration =  configuration.getInt(Constants.Security.TOKEN_EXPIRATION);
    // Test expiration time in seconds
    assertEquals(expectedExpiration / 1000, expiration);

    // Test that the server passes back an AccessToken object which can be decoded correctly.
    String encodedToken = responseJson.get(ExternalAuthenticationServer.ResponseFields.ACCESS_TOKEN).getAsString();
    AccessToken token = tokenCodec.decode(Base64.decodeBase64(encodedToken));
    assertEquals("admin", token.getIdentifier().getUsername());
    LOG.info("AccessToken got from ExternalAuthenticationServer is: " + encodedToken);
  }

  /**
   * Test an unauthorized request to server.
   * @throws Exception
   */
  @Test
  public void testInvalidAuthentication() throws Exception {
    HttpClient client = getHTTPClient();
    String uri = String.format("%s://%s:%d/%s", getProtocol(), server.getSocketAddress().getAddress().getHostAddress(),
                               server.getSocketAddress().getPort(), GrantAccessToken.Paths.GET_TOKEN);
    HttpGet request = new HttpGet(uri);
    request.addHeader("Authorization", "xxxxx");

    HttpResponse response = client.execute(request);

    // Request is Unauthorized
    assertEquals(401, response.getStatusLine().getStatusCode());
    verify(TEST_AUDIT_LOGGER, timeout(10000).atLeastOnce()).trace(contains("401"));
  }

  /**
   * Test an unauthorized status request to server.
   * @throws Exception
   */
  @Test
  public void testStatusResponse() throws Exception {
    HttpClient client = getHTTPClient();
    String uri = String.format("%s://%s:%d/%s", getProtocol(), server.getSocketAddress().getAddress().getHostAddress(),
                               server.getSocketAddress().getPort(), Constants.EndPoints.STATUS);
    HttpGet request = new HttpGet(uri);

    HttpResponse response = client.execute(request);

    // Status request is authorized without any extra headers
    assertEquals(200, response.getStatusLine().getStatusCode());

  }

  /**
   * Test getting a long lasting Access Token.
   * @throws Exception
   */
  @Test
  public void testExtendedToken() throws Exception {
    HttpClient client = getHTTPClient();
    String uri = String.format("%s://%s:%d/%s", getProtocol(), server.getSocketAddress().getAddress().getHostAddress(),
                               server.getSocketAddress().getPort(), GrantAccessToken.Paths.GET_EXTENDED_TOKEN);
    HttpGet request = new HttpGet(uri);
    request.addHeader("Authorization", "Basic YWRtaW46cmVhbHRpbWU=");
    HttpResponse response = client.execute(request);

    assertEquals(200, response.getStatusLine().getStatusCode());

    // Test correct response body
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteStreams.copy(response.getEntity().getContent(), bos);
    String responseBody = bos.toString("UTF-8");
    bos.close();

    JsonParser parser = new JsonParser();
    JsonObject responseJson = (JsonObject) parser.parse(responseBody);
    long expiration = responseJson.get(ExternalAuthenticationServer.ResponseFields.EXPIRES_IN).getAsLong();

    long expectedExpiration =  configuration.getInt(Constants.Security.EXTENDED_TOKEN_EXPIRATION);
    // Test expiration time in seconds
    assertEquals(expectedExpiration / 1000, expiration);

    // Test that the server passes back an AccessToken object which can be decoded correctly.
    String encodedToken = responseJson.get(ExternalAuthenticationServer.ResponseFields.ACCESS_TOKEN).getAsString();
    AccessToken token = tokenCodec.decode(Base64.decodeBase64(encodedToken));
    assertEquals("admin", token.getIdentifier().getUsername());
    LOG.info("AccessToken got from ExternalAuthenticationServer is: " + encodedToken);
  }

  /**
   * Test that invalid paths return a 404 Not Found.
   * @throws Exception
   */
  @Test
  public void testInvalidPath() throws Exception {
    HttpClient client = getHTTPClient();
    String uri = String.format("%s://%s:%d/%s", getProtocol(), server.getSocketAddress().getAddress().getHostAddress(),
                               server.getSocketAddress().getPort(), "invalid");
    HttpGet request = new HttpGet(uri);
    request.addHeader("Authorization", "Basic YWRtaW46cmVhbHRpbWU=");
    HttpResponse response = client.execute(request);

    assertEquals(404, response.getStatusLine().getStatusCode());

  }

  /**
   * Test that the service is discoverable.
   * @throws Exception
   */
  @Test
  public void testServiceRegistration() throws Exception {
    Iterable<Discoverable> discoverables = discoveryServiceClient.discover(Constants.Service.EXTERNAL_AUTHENTICATION);

    Set<SocketAddress> addresses = Sets.newHashSet();
    for (Discoverable discoverable : discoverables) {
      addresses.add(discoverable.getSocketAddress());
    }

    Assert.assertTrue(addresses.contains(server.getSocketAddress()));
  }
}
