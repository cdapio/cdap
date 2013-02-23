package com.continuuity.gateway.util;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;

/**
 * Class that is responsible for creating SSLContext
 */
public class SecureSSLContextFactory {

  private final SSLContext serverContext;
  private static final String PROTOCOL = "TLS";
  private static final String KEYSTORE_TYPE = "JKS";
  private SSLContext SERVER_CONTEXT;

  public SecureSSLContextFactory(File certKey, String certKeyPassword, String algorithm)
    throws KeyStoreException, CertificateException, NoSuchAlgorithmException,
    IOException, UnrecoverableKeyException, KeyManagementException {

    FileInputStream fileInputStream = new FileInputStream(certKey);
    try {
      KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
      keyStore.load(fileInputStream, certKeyPassword.toCharArray());
      // Set up key manager factory to use our key store
      KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(algorithm);
      keyManagerFactory.init(keyStore, certKeyPassword.toCharArray());
      // Initialize the SSLContext to work with our key managers.
      serverContext = SSLContext.getInstance(PROTOCOL);
      serverContext.init(keyManagerFactory.getKeyManagers(), null, null);
    }
    finally{
      fileInputStream.close();
    }
  }

  public SSLContext getServerContext() {
    return SERVER_CONTEXT;
  }
}
