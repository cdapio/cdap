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

package co.cask.cdap.security.tools;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.security.KeyStore;
import java.security.cert.X509Certificate;

public class KeyStoresTest {
  private static final String CERT_ALIAS = "cert";
  private static final String DISTINGUISHED_NAME = "CN=CDAP, L=Palo Alto, C=US";
  private static final String SIGNATURE_ALGORITHM = "MD5withRSA";
  private static final String SSL_KEYSTORE_TYPE = "JKS";
  private static final String CERTIFICATE_TYPE = "X.509";
  private static final String SSL_PASSWORD = "pass";

  @Test
  public void testGetSSLKeyStore() throws Exception {
    SConfiguration sConf = SConfiguration.create();
    sConf.set(Constants.Security.SSL.KEYSTORE_PASSWORD, SSL_PASSWORD);
    KeyStore ks = KeyStores.generatedCertKeyStore(sConf, SSL_PASSWORD);
    Assert.assertEquals(SSL_KEYSTORE_TYPE, ks.getType());
    Assert.assertEquals(CERT_ALIAS, ks.aliases().nextElement());
    Assert.assertEquals(1, ks.size());
    Assert.assertTrue(ks.getCertificate(CERT_ALIAS) instanceof X509Certificate);

    X509Certificate cert = (X509Certificate) ks.getCertificate(CERT_ALIAS);
    cert.checkValidity(); // throws an exception on failure
    Assert.assertEquals(CERTIFICATE_TYPE, cert.getType());
    Assert.assertEquals(SIGNATURE_ALGORITHM, cert.getSigAlgName());
    Assert.assertEquals(DISTINGUISHED_NAME, cert.getIssuerDN().getName());
    Assert.assertEquals(3, cert.getVersion());
  }
}
