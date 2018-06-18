/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.security.tools;

import co.cask.http.NettyHttpService;
import co.cask.http.SSLHandlerFactory;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

/**
 * A builder style class to help enabling HTTPS for server and client.
 */
public final class HttpsEnabler {


  private KeyStore keyStore;
  private Supplier<char[]> keystorePasswordSupplier;
  private KeyStore trustStore;

  /**
   * Sets the keystore to use for encryption.
   * For server side HTTPS enabling via the {@link #enable(NettyHttpService.Builder)} method, this must be set.
   * This is optional for client side and if it is set, then client side authentication will be enabled.
   *
   * @param keyStore the {@link KeyStore} to use
   * @param keystorePasswordSupplier a {@link Supplier} to provide the password for the keystore
   * @return this instance
   */
  public HttpsEnabler setKeyStore(KeyStore keyStore, Supplier<char[]> keystorePasswordSupplier) {
    this.keyStore = keyStore;
    this.keystorePasswordSupplier = keystorePasswordSupplier;
    return this;
  }

  /**
   * Sets the trust store to use for verification.
   * If a trust store is provided for server side HTTPS, the server will authenticate the client based on
   * the trust store, otherwise it will accept all clients.
   * If a trust store is provided for client side HTTPS, the client will verify the server identify based on
   * the trust store, otherwise it will trust any server.
   *
   * @param trustStore the {@link KeyStore} containing certificates to be trusted.
   * @return this instance
   */
  public HttpsEnabler setTrustStore(KeyStore trustStore) {
    this.trustStore = trustStore;
    return this;
  }

  /**
   * Enables HTTPS for the given {@link HttpsURLConnection} based on the configuration in this class
   *
   * @param urlConn the {@link HttpsURLConnection} to update
   * @param trustAny if {@link true} it will trust any server;
   *                 otherwise it will based on the trust store if it is set, or use the default trust chain.
   * @return the urlConn from the parameter
   * @throws RuntimeException if failed to enable HTTPS
   */
  public HttpsURLConnection enable(HttpsURLConnection urlConn, boolean trustAny) {
    try {
      KeyManagerFactory kmf = getKeyManagerFactory();
      TrustManagerFactory tmf = getTrustManagerFactory(trustAny);

      SSLContext sslContext = SSLContext.getInstance("SSL");
      sslContext.init(kmf == null ? null : kmf.getKeyManagers(),
                      tmf == null ? null : tmf.getTrustManagers(),
                      new SecureRandom());

      urlConn.setSSLSocketFactory(sslContext.getSocketFactory());
      urlConn.setHostnameVerifier((s, sslSession) -> true);

      return urlConn;
    } catch (UnrecoverableKeyException | NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
      throw new RuntimeException("Failed to enable HTTPS for HttpsURLConnection", e);
    }
  }

  /**
   * Enables HTTPS for the given {@link NettyHttpService.Builder} based on the configuration in this class.
   *
   * @param builder the builder to update
   * @param <T> type of the builder
   * @return the builder from the parameter
   * @throws IllegalArgumentException if keystore is missing
   * @throws RuntimeException if failed to enable HTTPS
   */
  public <T extends NettyHttpService.Builder> T enable(T builder) {
    try {
      KeyManagerFactory kmf = getKeyManagerFactory();
      if (kmf == null) {
        throw new IllegalArgumentException("Missing keystore to enable HTTPS for NettyHttpService");
      }

      // Initialize the SslContext to work with our key managers.
      SslContextBuilder contextBuilder = SslContextBuilder.forServer(kmf);
      TrustManagerFactory tmf = getTrustManagerFactory(false);
      if (tmf != null) {
        contextBuilder = contextBuilder.trustManager(tmf);
      }

      builder.enableSSL(new CustomSSLHandlerFactory(contextBuilder.build(), tmf != null));

      return builder;
    } catch (UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException | SSLException e) {
      throw new RuntimeException("Failed to enable HTTPS for NettyHttpService", e);
    }
  }

  /**
   * Returns a {@link KeyManagerFactory} based on the configuration in this class.
   *
   * @return a {@link KeyManagerFactory} or {@code null} if no key store is configured
   */
  @Nullable
  private KeyManagerFactory getKeyManagerFactory() throws UnrecoverableKeyException,
    NoSuchAlgorithmException, KeyStoreException {
    if (keyStore == null) {
      return null;
    }

    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, keystorePasswordSupplier.get());
    return kmf;
  }

  /**
   * Returns a {@link TrustManagerFactory} based on the configuration in this class.
   *
   * @param trustAny if {@code true} the {@link TrustManagerFactory} returned will trust any entity
   * @return a {@link TrustManagerFactory} or {@code null} if no trust store is configured.
   */
  @Nullable
  private TrustManagerFactory getTrustManagerFactory(boolean trustAny) throws NoSuchAlgorithmException,
    KeyStoreException {
    if (trustAny) {
      return InsecureTrustManagerFactory.INSTANCE;
    }

    if (trustStore == null) {
      return null;
    }

    TrustManagerFactory tmfFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmfFactory.init(trustStore);
    return tmfFactory;
  }

  /**
   * Private class for overriding the {@link SSLHandlerFactory#create(ByteBufAllocator)} method in order to
   * enable client side authentication.
   */
  private static final class CustomSSLHandlerFactory extends SSLHandlerFactory {

    private final boolean clientAuthEnabled;

    CustomSSLHandlerFactory(SslContext sslContext, boolean clientAuthEnabled) {
      super(sslContext);
      this.clientAuthEnabled = clientAuthEnabled;
    }

    @Override
    public SslHandler create(ByteBufAllocator bufferAllocator) {
      SslHandler handler = super.create(bufferAllocator);
      handler.engine().setNeedClientAuth(clientAuthEnabled);
      return handler;
    }
  }
}
