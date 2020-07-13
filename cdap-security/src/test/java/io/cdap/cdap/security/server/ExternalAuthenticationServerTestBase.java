/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.security.server;

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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.guice.SecurityModules;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.SocketAddress;
import java.net.URL;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.security.auth.login.Configuration;

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
  private static Codec<AccessToken> tokenCodec;
  private static DiscoveryServiceClient discoveryServiceClient;

  private static final Logger TEST_AUDIT_LOGGER = mock(Logger.class);

  // Needs to be set by derived classes.
  protected static CConfiguration configuration;
  protected static SConfiguration sConfiguration;

  protected abstract String getProtocol();

  @Nullable
  protected abstract Map<String, String> getAuthRequestHeader() throws Exception;

  protected abstract String getAuthenticatedUserName() throws Exception;

  protected abstract void startExternalAuthenticationServer() throws Exception;

  protected abstract void stopExternalAuthenticationServer() throws Exception;

  protected abstract CConfiguration getConfiguration(CConfiguration cConf);

  protected HttpURLConnection openConnection(URL url) throws Exception {
    return (HttpURLConnection) url.openConnection();
  }

  protected void setup() throws Exception {
    Assert.assertNotNull("CConfiguration needs to be set by derived classes", configuration);
    // Intentionally set "security.auth.server.announce.urls" to invalid
    // values verify that they are not used by external authentication server
    configuration.set(Constants.Security.AUTH_SERVER_ANNOUNCE_URLS, "invalid.urls");

    Module securityModule = Modules.override(new SecurityModules().getInMemoryModules()).with(
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
                                             new InMemoryDiscoveryModule());
    server = injector.getInstance(ExternalAuthenticationServer.class);
    tokenCodec = injector.getInstance(AccessTokenCodec.class);
    discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);

    startExternalAuthenticationServer();

    server.startAndWait();
    LOG.info("Auth server running on address {}", server.getSocketAddress());
    TimeUnit.SECONDS.sleep(3);
  }

  protected void tearDown() throws Exception {
    stopExternalAuthenticationServer();
    server.stopAndWait();
    // Clear any security properties for zookeeper.
    System.clearProperty(Constants.External.Zookeeper.ENV_AUTH_PROVIDER_1);
    Configuration.setConfiguration(null);
  }

  /**
   * Returns the full URL for the given request path
   */
  protected URL getURL(String path) throws MalformedURLException {
    InetSocketAddress serverAddr = server.getSocketAddress();
    while (path.startsWith("/")) {
      path = path.substring(1);
    }
    return new URL(String.format("%s://%s:%d/%s", getProtocol(), serverAddr.getHostName(), serverAddr.getPort(), path));
  }

  public ExternalAuthenticationServer getServer() {
    return server;
  }

  public DiscoveryServiceClient getDiscoveryServiceClient() {
    return discoveryServiceClient;
  }

  /**
   * Test an authorized request to server.
   *
   * @throws Exception
   */
  @Test
  public void testValidAuthentication() throws Exception {
    HttpURLConnection urlConn = openConnection(getURL(GrantAccessToken.Paths.GET_TOKEN));
    try {
      Optional.ofNullable(getAuthRequestHeader()).ifPresent(m -> m.forEach(urlConn::addRequestProperty));
      Assert.assertEquals(200, urlConn.getResponseCode());

      verify(TEST_AUDIT_LOGGER, timeout(10000).atLeastOnce()).trace(contains(getAuthenticatedUserName()));

      // Test correct headers being returned
      String cacheControlHeader = urlConn.getHeaderField(HttpHeaderNames.CACHE_CONTROL.toString());
      String pragmaHeader = urlConn.getHeaderField(HttpHeaderNames.PRAGMA.toString());
      String contentType = urlConn.getHeaderField(HttpHeaderNames.CONTENT_TYPE.toString());

      Assert.assertEquals("no-store", cacheControlHeader);
      Assert.assertEquals("no-cache", pragmaHeader);
      Assert.assertEquals("application/json;charset=UTF-8", contentType);

      // Test correct response body
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (InputStream is = urlConn.getInputStream()) {
        ByteStreams.copy(is, bos);
      }
      String responseBody = bos.toString("UTF-8");

      JsonParser parser = new JsonParser();
      JsonObject responseJson = (JsonObject) parser.parse(responseBody);
      String tokenType = responseJson.get(ExternalAuthenticationServer.ResponseFields.TOKEN_TYPE).toString();
      long expiration = responseJson.get(ExternalAuthenticationServer.ResponseFields.EXPIRES_IN).getAsLong();

      Assert.assertEquals(String.format("\"%s\"", ExternalAuthenticationServer.ResponseFields.TOKEN_TYPE_BODY),
                          tokenType);

      long expectedExpiration = configuration.getInt(Constants.Security.TOKEN_EXPIRATION);
      // Test expiration time in seconds
      Assert.assertEquals(expectedExpiration / 1000, expiration);

      // Test that the server passes back an AccessToken object which can be decoded correctly.
      String encodedToken = responseJson.get(ExternalAuthenticationServer.ResponseFields.ACCESS_TOKEN).getAsString();
      AccessToken token = tokenCodec.decode(Base64.getDecoder().decode(encodedToken));
      Assert.assertEquals(getAuthenticatedUserName(), token.getIdentifier().getUsername());
      LOG.info("AccessToken got from ExternalAuthenticationServer is: " + encodedToken);
    } finally {
      urlConn.disconnect();
    }
  }

  /**
   * Test an unauthorized request to server.
   *
   * @throws Exception
   */
  @Test
  public void testInvalidAuthentication() throws Exception {
    HttpURLConnection urlConn = openConnection(getURL(GrantAccessToken.Paths.GET_TOKEN));
    try {
      Optional.ofNullable(getAuthRequestHeader())
        .ifPresent(m -> m.forEach((k, v) -> urlConn.addRequestProperty(k, "xxxxx")));

      // Request is Unauthorized
      Assert.assertEquals(401, urlConn.getResponseCode());
      verify(TEST_AUDIT_LOGGER, timeout(10000).atLeastOnce()).trace(contains("401"));
    } finally {
      urlConn.disconnect();
    }
  }

  /**
   * Test an unauthorized status request to server.
   *
   * @throws Exception
   */
  @Test
  public void testStatusResponse() throws Exception {
    HttpURLConnection urlConn = openConnection(getURL(Constants.EndPoints.STATUS));
    try {
      // Status request is authorized without any extra headers
      Assert.assertEquals(200, urlConn.getResponseCode());
    } finally {
      urlConn.disconnect();
    }
  }

  /**
   * Test getting a long lasting Access Token.
   *
   * @throws Exception
   */
  @Test
  public void testExtendedToken() throws Exception {
    HttpURLConnection urlConn = openConnection(getURL(GrantAccessToken.Paths.GET_EXTENDED_TOKEN));
    try {
      Optional.ofNullable(getAuthRequestHeader()).ifPresent(m -> m.forEach(urlConn::addRequestProperty));

      Assert.assertEquals(200, urlConn.getResponseCode());

      // Test correct response body
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (InputStream is = urlConn.getInputStream()) {
        ByteStreams.copy(is, bos);
      }
      String responseBody = bos.toString("UTF-8");

      JsonParser parser = new JsonParser();
      JsonObject responseJson = (JsonObject) parser.parse(responseBody);
      long expiration = responseJson.get(ExternalAuthenticationServer.ResponseFields.EXPIRES_IN).getAsLong();

      long expectedExpiration = configuration.getInt(Constants.Security.EXTENDED_TOKEN_EXPIRATION);
      // Test expiration time in seconds
      Assert.assertEquals(expectedExpiration / 1000, expiration);

      // Test that the server passes back an AccessToken object which can be decoded correctly.
      String encodedToken = responseJson.get(ExternalAuthenticationServer.ResponseFields.ACCESS_TOKEN).getAsString();
      AccessToken token = tokenCodec.decode(Base64.getDecoder().decode(encodedToken));
      Assert.assertEquals(getAuthenticatedUserName(), token.getIdentifier().getUsername());
      LOG.info("AccessToken got from ExternalAuthenticationServer is: " + encodedToken);
    } finally {
      urlConn.disconnect();
    }
  }

  /**
   * Test that invalid paths return a 404 Not Found.
   *
   * @throws Exception
   */
  @Test
  public void testInvalidPath() throws Exception {
    HttpURLConnection urlConn = openConnection(getURL("invalid"));
    try {
      Optional.ofNullable(getAuthRequestHeader()).ifPresent(m -> m.forEach(urlConn::addRequestProperty));
      Assert.assertEquals(404, urlConn.getResponseCode());
    } finally {
      urlConn.disconnect();
    }
  }

  /**
   * Test that the service is discoverable.
   *
   */
  @Test
  public void testServiceRegistration() {
    Iterable<Discoverable> discoverables = discoveryServiceClient.discover(Constants.Service.EXTERNAL_AUTHENTICATION);

    Set<SocketAddress> addresses = Sets.newHashSet();
    for (Discoverable discoverable : discoverables) {
      addresses.add(discoverable.getSocketAddress());
    }

    Assert.assertTrue(addresses.contains(server.getSocketAddress()));
  }
}
