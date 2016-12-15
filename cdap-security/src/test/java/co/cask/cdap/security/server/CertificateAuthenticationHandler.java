/*
 * Copyright © 2016 Cask Data, Inc.
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

import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.authentication.ClientCertAuthenticator;

import javax.security.auth.login.Configuration;

/**
 * An Authentication Handler that support mutual TLS based authentication. The
 * server provide the client with its certificate during the SSL handshake,
 * after which the server requests the client to provide a certificate signed by
 * a trusted authority. The Handler validates the client's certificate and checks for
 * identity based on the realm file
 */
public class CertificateAuthenticationHandler extends AbstractAuthenticationHandler {

  public static final String AUTH_SSL_CONFIG_BASE = "security.auth.server.ssl.";

  /**
   * Configure the Jetty {@link ClientCertAuthenticator} by setting the
   * Truststore.
   *
   * @param clientCertAuthenticator
   */
  private void setupClientCertAuthenticator(ClientCertAuthenticator clientCertAuthenticator) {
    String trustStorePath = handlerProps.get(AUTH_SSL_CONFIG_BASE.concat("truststore.path"));
    String trustStorePassword = handlerProps.get(AUTH_SSL_CONFIG_BASE.concat("truststore.password"));
    String trustStoreType = handlerProps.get(AUTH_SSL_CONFIG_BASE.concat("truststore.type"));

    if (StringUtils.isNotEmpty(trustStorePath)) {
      clientCertAuthenticator.setTrustStore(trustStorePath);
    }

    if (StringUtils.isNotEmpty(trustStorePassword)) {
      clientCertAuthenticator.setTrustStorePassword(trustStorePassword);
    }
    
    if (StringUtils.isNotEmpty(trustStoreType)) {
      clientCertAuthenticator.setTrustStoreType(trustStoreType);
    }
    clientCertAuthenticator.setValidateCerts(true);
  }

  @Override
  protected LoginService getHandlerLoginService() {
    return new MTLSLoginService(handlerProps.get("realmfile"));
  }

  @Override
  protected Authenticator getHandlerAuthenticator() {
    ClientCertAuthenticator clientCertAuthenticator = new ClientCertAuthenticator();
    setupClientCertAuthenticator(clientCertAuthenticator);
    return clientCertAuthenticator;
  }

  @Override
  public IdentityService getHandlerIdentityService() {
    return new DefaultIdentityService();
  }

  @Override
  protected Configuration getLoginModuleConfiguration() {
    return null;
  }

}
