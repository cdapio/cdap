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
 * the License
 */

package co.cask.cdap.security.server;

import com.google.common.base.Throwables;
import org.eclipse.jetty.plus.jaas.spi.LdapLoginModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * A custom {@link LoginModule} that does LDAP authentication. It allows the disabling of SSL
 * certificate verification for connections between the {@link ExternalAuthenticationServer} and an LDAP instance.
 */
public class LDAPLoginModule extends LdapLoginModule {
  private static final Logger LOG = LoggerFactory.getLogger(LDAPLoginModule.class);

  /**
   * A {@link SocketFactory} that trusts all SSL certificates.
   */
  public static class TrustAllSSLSocketFactory extends SocketFactory {
    private final SocketFactory trustAllFactory;

    private TrustAllSSLSocketFactory() {
      TrustManager[] trustManagers = new TrustManager[] { new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
          // no-op
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
          // no-op
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return null;
        }
      }};

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
     * @see SocketFactory#getDefault()
     */
    public static SocketFactory getDefault() {
      return new TrustAllSSLSocketFactory();
    }

    /**
     * @see SocketFactory#createSocket(String, int)
     */
    public Socket createSocket(String host, int port) throws IOException  {
      return trustAllFactory.createSocket(host, port);
    }

    /**
     * @see SocketFactory#createSocket(InetAddress, int)
     */
    public Socket createSocket(InetAddress address, int port) throws IOException {
      return trustAllFactory.createSocket(address, port);
    }

    /**
     * @see SocketFactory#createSocket(String, int, InetAddress, int)
     */
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
      return trustAllFactory.createSocket(host, port, localHost, localPort);
    }

    /**
     * @see SocketFactory#createSocket(InetAddress, int, InetAddress, int)
     */
    public Socket createSocket(InetAddress address, int port,
                               InetAddress localAddress, int localPort) throws IOException {
      return trustAllFactory.createSocket(address, port, localAddress, localPort);
    }
  }

  @Override
  public Hashtable<Object, Object> getEnvironment() {
    Hashtable<Object, Object> table = super.getEnvironment();

    if (!LDAPAuthenticationHandler.getLdapSSLVerifyCertificate()) {
      table.put("java.naming.ldap.factory.socket", TrustAllSSLSocketFactory.class.getName());
    }
    return table;
  }
}
