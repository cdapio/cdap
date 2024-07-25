/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
 * the License
 */

package io.cdap.cdap.security.server;

import com.google.common.base.Throwables;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Hashtable;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.spi.LoginModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A custom {@link LoginModule} that does LDAP authentication. It allows the disabling of SSL
 * certificate verification for connections between the {@link ExternalAuthenticationServer} and an
 * LDAP instance.
 */
public class LdapLoginModule extends org.eclipse.jetty.jaas.spi.LdapLoginModule {

  private static final Logger LOG = LoggerFactory.getLogger(LdapLoginModule.class);

  /**
   * A {@link SocketFactory} that trusts all SSL certificates.
   */
  public static class TrustAllSslSocketFactory extends SocketFactory {

    private final SocketFactory trustAllFactory;

    private TrustAllSslSocketFactory() {
      TrustManager[] trustManagers = new TrustManager[]{ new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {
          // no-op
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {
          // no-op
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return null;
        }
      }
      };

      try {
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustManagers, new SecureRandom());
        trustAllFactory = sc.getSocketFactory();
      } catch (GeneralSecurityException e) {
        LOG.error("Could not disable certificate verification for connections to LDAP.", e);
        throw Throwables.propagate(e);
      }
    }

    /**
     * Similar to method mentioned below.
     *
     * @see SocketFactory#getDefault()
     */
    public static SocketFactory getDefault() {
      return new TrustAllSslSocketFactory();
    }

    /**
     * Overridden method.
     *
     * @see SocketFactory#createSocket(String, int)
     */
    @Override
    public Socket createSocket(String host, int port) throws IOException {
      return trustAllFactory.createSocket(host, port);
    }

    /**
     * Overridden method.
     *
     * @see SocketFactory#createSocket(InetAddress, int)
     */
    @Override
    public Socket createSocket(InetAddress address, int port) throws IOException {
      return trustAllFactory.createSocket(address, port);
    }

    /**
     * Overridden method.
     *
     * @see SocketFactory#createSocket(String, int, InetAddress, int)
     */
    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
        throws IOException {
      return trustAllFactory.createSocket(host, port, localHost, localPort);
    }

    /**
     * Overridden method.
     *
     * @see SocketFactory#createSocket(InetAddress, int, InetAddress, int)
     */
    @Override
    public Socket createSocket(InetAddress address, int port,
        InetAddress localAddress, int localPort) throws IOException {
      return trustAllFactory.createSocket(address, port, localAddress, localPort);
    }
  }

  @Override
  public Hashtable<Object, Object> getEnvironment() {
    Hashtable<Object, Object> table = super.getEnvironment();

    if (!LdapAuthenticationHandler.getLdapSslVerifyCertificate()) {
      table.put("java.naming.ldap.factory.socket", TrustAllSslSocketFactory.class.getName());
    }
    return table;
  }
}
