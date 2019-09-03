/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.common.security;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.KeyStore;
import javax.net.ssl.HttpsURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

public class HttpsEnablerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  /**
   * Testing https server with the client always trust the server.
   */
  @Test
  public void testAlwaysTrustedHttpsServer() throws Exception {
    testServer(false, true);
  }

  /**
   * Testing https server with the client validating and trust the server.
   */
  @Test
  public void testValidHttpsServer() throws Exception {
    testServer(true, false);
  }

  /**
   * Testing https server with the client not trusting the server.
   */
  @Test (expected = IOException.class)
  public void testInvalidHttpsServer() throws Exception {
    testServer(false, false);
  }

  /**
   * Testing client side authentication in HTTPS.
   */
  @Test
  public void testClientSideAuthentication() throws Exception {
    testClientAuth(true, true);
  }

  /**
   * Testing client side authentication with an untrusted client side cert.
   */
  @Test (expected = IOException.class)
  public void testInvalidClientAuthentication() throws Exception {
    testClientAuth(true, false);
  }

  /**
   * Testing client side authentication with the client missing the cert.
   */
  @Test (expected = IOException.class)
  public void testMissingClientAuthentication() throws Exception {
    testClientAuth(false, true);
  }

  @Test
  public void testConfigure() throws Exception {
    String password = "testing";
    KeyStore keyStore = KeyStores.generatedCertKeyStore(1, password);
    File pemFile = KeyStoresTest.writePEMFile(TEMP_FOLDER.newFile(), keyStore, KeyStores.CERT_ALIAS, password);

    CConfiguration cConf = CConfiguration.create();
    SConfiguration sConf = SConfiguration.create();

    cConf.set(Constants.Security.SSL.INTERNAL_CERT_PATH, pemFile.getAbsolutePath());
    sConf.set(Constants.Security.SSL.INTERNAL_CERT_PASSWORD, password);

    // Start the server with SSL enabled
    NettyHttpService httpService = new HttpsEnabler()
      .configureKeyStore(cConf, sConf)
      .enable(
        NettyHttpService.builder("test")
          .setHttpHandlers(new PingHandler()))
      .build();

    httpService.start();

    try {
      // Create a client that trust the server
      InetSocketAddress address = httpService.getBindAddress();
      URL url = new URL(String.format("https://%s:%d/ping", address.getHostName(), address.getPort()));
      HttpsURLConnection urlConn = new HttpsEnabler().configureTrustStore(cConf, sConf)
        .enable((HttpsURLConnection) url.openConnection());

      Assert.assertEquals(200, urlConn.getResponseCode());
    } finally {
      httpService.stop();
    }
  }

  /**
   * Private method to verify https connection.
   *
   * @param useTrustStore {@code true} to have the client use a trust store that contains the certificate of the server
   * @param trustAll {@code true} to have the client trust any https server
   */
  private void testServer(boolean useTrustStore, boolean trustAll) throws Exception {
    String ksPass = "xyz";
    KeyStore keyStore = KeyStores.generatedCertKeyStore(1, ksPass);

    // Start the http server
    NettyHttpService httpService = new HttpsEnabler()
      .setKeyStore(keyStore, ksPass::toCharArray)
      .enable(
        NettyHttpService.builder("test")
          .setHttpHandlers(new PingHandler())
      ).build();

    httpService.start();

    try {
      // Verify that it can be hit with HTTPS
      InetSocketAddress address = httpService.getBindAddress();
      URL url = new URL(String.format("https://%s:%d/ping", address.getHostName(), address.getPort()));

      HttpsEnabler enabler = new HttpsEnabler().setTrustAll(trustAll);

      // Optionally validates the server
      if (useTrustStore) {
        enabler.setTrustStore(KeyStores.createTrustStore(keyStore));
      }

      HttpsURLConnection urlConn = enabler.enable((HttpsURLConnection) url.openConnection());
      Assert.assertEquals(200, urlConn.getResponseCode());
    } finally {
      httpService.stop();
    }
  }

  /**
   * Private method to verify client side authentication.
   *
   * @param useClientAuth if {@code true}, enable client side authentication from the https client
   * @param trustClient if {@code true}, server trust the client using the client trust store
   */
  private void testClientAuth(boolean useClientAuth, boolean trustClient) throws Exception {
    String ksPass = "abc";
    KeyStore serverKeyStore = KeyStores.generatedCertKeyStore(1, ksPass);
    KeyStore clientKeyStore = KeyStores.generatedCertKeyStore(1, ksPass);

    // Start the http server
    HttpsEnabler serverEnabler = new HttpsEnabler().setKeyStore(serverKeyStore, ksPass::toCharArray);
    if (trustClient) {
      serverEnabler.setTrustStore(KeyStores.createTrustStore(clientKeyStore));
    } else {
      // Generates a different trust store used by the server
      serverEnabler.setTrustStore(KeyStores.createTrustStore(KeyStores.generatedCertKeyStore(1, ksPass)));
    }

    NettyHttpService httpService = serverEnabler.enable(
        NettyHttpService.builder("test")
          .setHttpHandlers(new PingHandler())
      ).build();

    httpService.start();

    try {
      // Hit the server with an optional client side authentication
      InetSocketAddress address = httpService.getBindAddress();
      URL url = new URL(String.format("https://%s:%d/ping", address.getHostName(), address.getPort()));

      HttpsEnabler clientEnabler = new HttpsEnabler().setTrustAll(true);
      if (useClientAuth) {
        clientEnabler.setKeyStore(clientKeyStore, ksPass::toCharArray);
      }

      HttpsURLConnection urlConn = clientEnabler.enable((HttpsURLConnection) url.openConnection());
      Assert.assertEquals(200, urlConn.getResponseCode());
    } finally {
      httpService.stop();
    }
  }


  /**
   * Handler class that exposes a /ping endpoint for testing.
   */
  public static final class PingHandler extends AbstractHttpHandler {
    @GET
    @Path("/ping")
    public void ping(HttpRequest request, HttpResponder responder) {
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }
}
