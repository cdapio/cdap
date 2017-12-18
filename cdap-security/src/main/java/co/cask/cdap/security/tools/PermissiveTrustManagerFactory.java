/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.X509Certificate;
import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactorySpi;
import javax.net.ssl.X509TrustManager;

/**
 * {@link TrustManagerFactorySpi} which accepts any certificate. This is generally a bad thing to do as it leaves the
 * system exposed to a man in the middle attack.
 */
public class PermissiveTrustManagerFactory extends TrustManagerFactorySpi {
  private static final Logger LOG = LoggerFactory.getLogger(PermissiveTrustManagerFactory.class);

  private static final TrustManager DUMMY_TRUST_MANAGER = new X509TrustManager() {
    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) {
      // Trust all certificates.
      LOG.trace("Client certificate not being verified: " + chain[0].getSubjectDN());
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) {
      // Trust all certificates.
      LOG.trace("Server certificate not being verified: " + chain[0].getSubjectDN());
    }
  };

  public static TrustManager[] getTrustManagers() {
    return new TrustManager[] { DUMMY_TRUST_MANAGER };
  }

  @Override
  protected void engineInit(KeyStore keyStore) throws KeyStoreException {

  }

  @Override
  protected void engineInit(ManagerFactoryParameters managerFactoryParameters)
    throws InvalidAlgorithmParameterException {

  }

  @Override
  protected TrustManager[] engineGetTrustManagers() {
    return getTrustManagers();
  }
}
