/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.Security;
import javax.annotation.Nullable;
import javax.net.ssl.KeyManagerFactory;

/**
 * A class that encapsulates SSL Certificate Information
 */
public class SSLHandlerFactory extends co.cask.http.SSLHandlerFactory {

  private static final String ALGORITHM = "SunX509";

  private static SslContext createSslContext(KeyStore keyStore, String certificatePassword) {
    if (keyStore == null) {
      throw new IllegalArgumentException("KeyStore path is not configured");
    }
    if (certificatePassword == null) {
      throw new IllegalArgumentException("Certificate password is not configured");
    }

    String algorithm = Security.getProperty("ssl.KeyManagerFactory.algorithm");
    if (algorithm == null) {
      algorithm = ALGORITHM;
    }

    try {
      // Set up key manager factory to use our key store
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
      kmf.init(keyStore, certificatePassword.toCharArray());

      // Initialize the SslContext to work with our key managers.
      return SslContextBuilder.forServer(kmf).build();
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to initialize the server-side SSLContext", e);
    }
  }

  private static SslContext createSslContext(File keyStoreFile, String keyStoreType,
                                             String keyStorePassword, @Nullable String certificatePassword) {
    if (keyStoreFile == null) {
      throw new IllegalArgumentException("KeyStore path is not configured");
    }
    if (keyStoreType == null) {
      throw new IllegalArgumentException("KeyStore type is not configured");
    }
    if (keyStorePassword == null) {
      throw new IllegalArgumentException("KeyStore password is not Configured");
    }

    try {
      KeyStore keyStore = KeyStore.getInstance(keyStoreType);
      try (InputStream inputStream = new FileInputStream(keyStoreFile)) {
        keyStore.load(inputStream, keyStorePassword.toCharArray());
      }
      return createSslContext(keyStore, certificatePassword == null ? keyStorePassword : certificatePassword);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to initialize the server-side SSLContext", e);
    }
  }

  public SSLHandlerFactory(File keyStoreFile, String keyStoreType,
                           String keyStorePassword, @Nullable String certificatePassword) {
    super(createSslContext(keyStoreFile, keyStoreType, keyStorePassword, certificatePassword));
  }

  public SSLHandlerFactory(KeyStore keyStore, String password) {
    super(createSslContext(keyStore, password));
  }
}
