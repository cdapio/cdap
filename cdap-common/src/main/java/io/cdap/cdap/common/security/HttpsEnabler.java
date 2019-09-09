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

import com.google.common.base.Strings;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.http.NettyHttpService;
import io.cdap.http.SSLHandlerFactory;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.util.function.Supplier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

/**
 * A builder style class to help enabling HTTPS for server and client.
 */
public final class HttpsEnabler {

  private KeyManagerFactory keyManagerFactory;
  private TrustManagerFactory trustManagerFactory;
  private volatile SSLSocketFactory sslSocketFactory;

  /**
   * Configures keystore to use based on the given configurations. This method is intended for service to use
   * to enable https server.
   *
   * @param cConf the configuration for looking up certificate location
   * @param sConf the security configuration for looking up certificate password
   * @return this instance
   */
  public synchronized HttpsEnabler configureKeyStore(CConfiguration cConf, SConfiguration sConf) {
    String path = cConf.get(Constants.Security.SSL.INTERNAL_CERT_PATH);

    String password = Strings.isNullOrEmpty(path)
      ? KeyStores.generateRandomPassword()
      : sConf.get(Constants.Security.SSL.INTERNAL_CERT_PASSWORD, "");
    KeyStore keyStore = Strings.isNullOrEmpty(path)
      ? KeyStores.generatedCertKeyStore(KeyStores.VALIDITY, password)
      : KeyStores.createKeyStore(Paths.get(path), password);

    return setKeyStore(keyStore, password::toCharArray);
  }

  /**
   * Configures a trust store to use based on the given configuration. This method is intended for client to use
   * to trust a https service configured with the same configuration. If there is no certificate information
   * in the given configurations, no trust store will be configured.
   *
   * @param cConf the configuration for looking up certificate location
   * @param sConf the security configuration for looking up certificate password
   * @return this instance
   */
  public synchronized HttpsEnabler configureTrustStore(CConfiguration cConf, SConfiguration sConf) {
    String path = cConf.get(Constants.Security.SSL.INTERNAL_CERT_PATH);
    if (Strings.isNullOrEmpty(path)) {
      return this;
    }

    return setTrustStore(KeyStores.createTrustStore(
      KeyStores.createKeyStore(Paths.get(path), sConf.get(Constants.Security.SSL.INTERNAL_CERT_PASSWORD, ""))));
  }

  /**
   * Sets the keystore to use for encryption.
   * For server side HTTPS enabling via the {@link #enable(NettyHttpService.Builder)} method, this must be set.
   * This is optional for client side and if it is set, then client side authentication will be enabled.
   *
   * @param keyStore the {@link KeyStore} to use
   * @param keystorePasswordSupplier a {@link Supplier} to provide the password for the keystore
   * @return this instance
   */
  public synchronized HttpsEnabler setKeyStore(KeyStore keyStore, Supplier<char[]> keystorePasswordSupplier) {
    try {
      keyManagerFactory = createKeyManagerFactory(keyStore, keystorePasswordSupplier);
      sslSocketFactory = null;
      return this;
    } catch (UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException e) {
      throw new RuntimeException("Failed to set key store", e);
    }
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
  public synchronized HttpsEnabler setTrustStore(KeyStore trustStore) {
    try {
      trustManagerFactory = createTrustManagerFactory(trustStore);
      sslSocketFactory = null;
      return this;
    } catch (NoSuchAlgorithmException | KeyStoreException e) {
      throw new RuntimeException("Failed to set trust store", e);
    }
  }

  /**
   * Sets to have the client trust all servers.
   *
   * @param trustAny if {@link true} it will trust any server;
   *                 otherwise it will based on the trust store if it is set through {@link #setTrustStore(KeyStore)},
   *                 or use the default trust chain.
   * @return this instance
   */
  public synchronized HttpsEnabler setTrustAll(boolean trustAny) {
    if (trustAny) {
      trustManagerFactory = InsecureTrustManagerFactory.INSTANCE;
      sslSocketFactory = null;
    }
    return this;
  }

  /**
   * Enables HTTPS for the given {@link HttpsURLConnection} based on the configuration in this class
   *
   * @param urlConn the {@link HttpsURLConnection} to update
   * @return the urlConn from the parameter
   * @throws RuntimeException if failed to enable HTTPS
   */
  public HttpsURLConnection enable(HttpsURLConnection urlConn) {
    try {
      urlConn.setSSLSocketFactory(getSSLSocketFactory());
      urlConn.setHostnameVerifier((s, sslSession) -> true);
      return urlConn;
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
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
      KeyManagerFactory kmf = keyManagerFactory;
      if (kmf == null) {
        throw new IllegalArgumentException("Missing keystore to enable HTTPS for NettyHttpService");
      }

      // Initialize the SslContext to work with our key managers.
      SslContextBuilder contextBuilder = SslContextBuilder.forServer(kmf);
      TrustManagerFactory tmf = this.trustManagerFactory;
      boolean hasTrustManager = tmf != null && tmf != InsecureTrustManagerFactory.INSTANCE;
      if (hasTrustManager) {
        contextBuilder = contextBuilder.trustManager(tmf);
      }

      builder.enableSSL(new CustomSSLHandlerFactory(contextBuilder.build(), hasTrustManager));

      return builder;
    } catch (SSLException e) {
      throw new RuntimeException("Failed to enable HTTPS for NettyHttpService", e);
    }
  }

  /**
   * Returns a {@link KeyManagerFactory} created from the given {@link KeyStore}.
   *
   * @param keyStore the {@link KeyStore} to use
   * @param passwordSupplier a {@link Supplier} to provide password for the given {@link KeyStore}.
   * @return a {@link KeyManagerFactory}
   */
  private KeyManagerFactory createKeyManagerFactory(KeyStore keyStore,
                                                    Supplier<char[]> passwordSupplier) throws UnrecoverableKeyException,
    NoSuchAlgorithmException, KeyStoreException {

    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, passwordSupplier.get());
    return kmf;
  }

  /**
   * Returns a {@link TrustManagerFactory} created from the given {@link KeyStore}.
   *
   * @param trustStore the {@link KeyStore} to use
   * @return a {@link TrustManagerFactory}
   */
  private TrustManagerFactory createTrustManagerFactory(KeyStore trustStore) throws NoSuchAlgorithmException,
    KeyStoreException {

    TrustManagerFactory tmfFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmfFactory.init(trustStore);
    return tmfFactory;
  }

  /**
   * Returns the {@link SSLSocketFactory} based on the latest configuration of this class.
   *
   * @return a {@link SSLSocketFactory}
   */
  private SSLSocketFactory getSSLSocketFactory() throws NoSuchAlgorithmException, KeyManagementException {
    SSLSocketFactory factory = sslSocketFactory;
    if (factory != null) {
      return factory;
    }
    synchronized (this) {
      factory = sslSocketFactory;
      if (factory != null) {
        return factory;
      }

      SSLContext sslContext = SSLContext.getInstance("SSL");
      KeyManagerFactory kmf = keyManagerFactory;
      TrustManagerFactory tmf = trustManagerFactory;

      sslContext.init(kmf == null ? null : kmf.getKeyManagers(),
                      tmf == null ? null : tmf.getTrustManagers(),
                      new SecureRandom());

      sslSocketFactory = factory = sslContext.getSocketFactory();
      return factory;
    }
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
