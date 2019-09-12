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

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMEncryptor;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.openssl.jcajce.JcePEMEncryptorBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;

public class KeyStoresTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final String CERTIFICATE_TYPE = "X.509";
  private static final String SSL_PASSWORD = "pass";

  @Test
  public void testGetSSLKeyStore() throws Exception {
    KeyStore ks = KeyStores.generatedCertKeyStore(KeyStores.VALIDITY, SSL_PASSWORD);
    Assert.assertEquals(KeyStores.SSL_KEYSTORE_TYPE, ks.getType());
    Assert.assertEquals(KeyStores.CERT_ALIAS, ks.aliases().nextElement());
    Assert.assertEquals(1, ks.size());
    Assert.assertTrue(ks.getCertificate(KeyStores.CERT_ALIAS) instanceof X509Certificate);

    X509Certificate cert = (X509Certificate) ks.getCertificate(KeyStores.CERT_ALIAS);
    cert.checkValidity(); // throws an exception on failure
    Assert.assertEquals(CERTIFICATE_TYPE, cert.getType());
    Assert.assertEquals(KeyStores.SIGNATURE_ALGORITHM, cert.getSigAlgName());
    Assert.assertEquals(KeyStores.DISTINGUISHED_NAME, cert.getIssuerDN().getName());
    Assert.assertEquals(3, cert.getVersion());
  }

  /**
   * Testing for trust store creation from key store.
   */
  @Test
  public void testCreateTrustStore() throws KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException {
    String password = "xyz";
    KeyStore keyStore = KeyStores.generatedCertKeyStore(1, password);
    KeyStore trustStore = KeyStores.createTrustStore(keyStore);

    List<String> aliases = Collections.list(keyStore.aliases());
    Assert.assertFalse(aliases.isEmpty());

    for (String alias : aliases) {
      // Should get the cert, but not the key
      Assert.assertNotNull(trustStore.getCertificate(alias));
      Assert.assertNull(trustStore.getKey(alias, password.toCharArray()));
    }
  }

  /**
   * Testing creation of {@link KeyStore} from PEM file.
   */
  @Test
  public void testPEMToKeyStore() throws Exception {
    // Write out PEM file. First without password for the key, then with password
    for (String password : new String[] { "", "testing" }) {
      // Generate a keystore and write out PEM blocks
      KeyStore keystore = KeyStores.generatedCertKeyStore(KeyStores.VALIDITY, password);
      Key key = keystore.getKey(KeyStores.CERT_ALIAS, password.toCharArray());

      File pemFile = writePEMFile(TEMP_FOLDER.newFile(), keystore, KeyStores.CERT_ALIAS, password);

      // Create a keystore from the PEM file
      KeyStore keystore2 = KeyStores.createKeyStore(pemFile.toPath(), password);
      Assert.assertEquals(key, keystore2.getKey(KeyStores.CERT_ALIAS, password.toCharArray()));
      Assert.assertEquals(keystore.getCertificate(KeyStores.CERT_ALIAS),
                          keystore2.getCertificate(KeyStores.CERT_ALIAS));
    }
  }

  /**
   * Writes a private key and certificate pair from a KeyStore to the given PEM file.
   */
  public static File writePEMFile(File pemFile, KeyStore keyStore, String alias, String password) throws Exception {
    Key key = keyStore.getKey(alias, password.toCharArray());
    Certificate certificate = keyStore.getCertificate(alias);

    try (PemWriter writer = new PemWriter(new FileWriter(pemFile))) {
      // Write out the key
      PEMEncryptor encryptor = null;
      if (!password.isEmpty()) {
        encryptor = new JcePEMEncryptorBuilder("AES-128-CBC")
          .setProvider(new BouncyCastleProvider())
          .setSecureRandom(new SecureRandom())
          .build(password.toCharArray());
      }
      writer.writeObject(new JcaMiscPEMGenerator(key, encryptor));

      // Write out the certificates
      writer.writeObject(new PemObject("CERTIFICATE", certificate.getEncoded()));
    }

    return pemFile;
  }
}
