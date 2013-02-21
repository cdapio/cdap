package com.continuuity.gateway.util;

import com.google.common.base.Throwables;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.security.KeyStore;
import java.security.Security;

/**
 *
 */
public class SecureSSLContextFactory {
  private static final String PROTOCOL = "TLS";
  private static final SSLContext SERVER_CONTEXT;
  private static final SSLContext CLIENT_CONTEXT;

  static {
    String algorithm = Security.getProperty("ssl.KeyManagerFactory.algorithm");

    if (algorithm == null) {
      algorithm = "RSA";
    }

    SSLContext serverContext = null;
    SSLContext clientContext = null;
    try {
      KeyStore keyStore = KeyStore.getInstance("JKS");
      keyStore.load(GatewayKeyStore.asInputStream(),GatewayKeyStore.getKeyStorePassword());

      // Set up key manager factory to use our key store
      KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(algorithm);
      keyManagerFactory.init(keyStore, GatewayKeyStore.getKeyStorePassword());

      // Initialize the SSLContext to work with our key managers.
      serverContext = SSLContext.getInstance(PROTOCOL);
      serverContext.init(keyManagerFactory.getKeyManagers(), null, null);
    } catch (Exception e) {
     throw Throwables.propagate(e);
    }
    try {
      clientContext = SSLContext.getInstance(PROTOCOL);
      clientContext.init(null,SecureGatewayTrustManagerFactory.getTrustManagers(),null);
    } catch (Exception e) {
     throw Throwables.propagate(e);
    }
    SERVER_CONTEXT = serverContext;
    CLIENT_CONTEXT = clientContext;
  }

  public static SSLContext getServerContext() {
    return SERVER_CONTEXT;
  }

  public static SSLContext getClientContext() {
    return CLIENT_CONTEXT;
  }

}
