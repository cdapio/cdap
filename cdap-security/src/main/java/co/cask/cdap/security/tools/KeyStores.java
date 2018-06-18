/*
 * Copyright © 2017 Cask Data, Inc.
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
import org.apache.commons.lang.time.DateUtils;
import sun.security.x509.AlgorithmId;
import sun.security.x509.CertificateAlgorithmId;
import sun.security.x509.CertificateIssuerName;
import sun.security.x509.CertificateSerialNumber;
import sun.security.x509.CertificateSubjectName;
import sun.security.x509.CertificateValidity;
import sun.security.x509.CertificateVersion;
import sun.security.x509.CertificateX509Key;
import sun.security.x509.X500Name;
import sun.security.x509.X509CertImpl;
import sun.security.x509.X509CertInfo;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Enumeration;

/**
 * Utility class with methods for generating a X.509 self signed certificate
 * and creating a Java key store with a self signed certificate.
 */
public final class KeyStores {

  public static final String SSL_KEYSTORE_TYPE = "JKS";

  private static final String KEY_PAIR_ALGORITHM = "RSA";
  private static final String SECURE_RANDOM_ALGORITHM = "SHA1PRNG";
  private static final String SECURE_RANDOM_PROVIDER = "SUN";

  /* This is based on https://www.ietf.org/rfc/rfc1779.txt
     CN      CommonName
     L       LocalityName
     ST      StateOrProvinceName
     O       OrganizationName
     OU      OrganizationalUnitName
     C       CountryName
     STREET  StreetAddress

     All fields are not required.
    */
  static final String DISTINGUISHED_NAME = "CN=CDAP, L=Palo Alto, C=US";
  static final String SIGNATURE_ALGORITHM = "SHA1withRSA";
  static final String CERT_ALIAS = "cert";
  private static final int KEY_SIZE = 2048;
  private static final int VALIDITY = 999;

  /* private constructor */
  private KeyStores() {}

  /**
   * Create a Java key store with a stored self-signed certificate.
   * @return Java keystore which has a self signed X.509 certificate
   */
  public static KeyStore generatedCertKeyStore(SConfiguration sConf, String password) {
    return generatedCertKeyStore(sConf.getInt(Constants.Security.SSL.CERT_VALIDITY, VALIDITY), password);
  }

  /**
   * Create a Java key store with a stored self-signed certificate.
   *
   * @param validityDays number of days that the cert will be valid for
   * @param password the password to protect the generated key store
   * @return Java keystore which has a self signed X.509 certificate
   */
  public static KeyStore generatedCertKeyStore(int validityDays, String password) {
    try {
      KeyPairGenerator keyGen = KeyPairGenerator.getInstance(KEY_PAIR_ALGORITHM);
      SecureRandom random = SecureRandom.getInstance(SECURE_RANDOM_ALGORITHM, SECURE_RANDOM_PROVIDER);
      keyGen.initialize(KEY_SIZE, random);
      // generate a key pair
      KeyPair pair = keyGen.generateKeyPair();

      X509Certificate cert = getCertificate(DISTINGUISHED_NAME, pair, validityDays, SIGNATURE_ALGORITHM);

      KeyStore keyStore = KeyStore.getInstance(SSL_KEYSTORE_TYPE);
      keyStore.load(null, password.toCharArray());
      keyStore.setKeyEntry(CERT_ALIAS, pair.getPrivate(), password.toCharArray(),
                           new java.security.cert.Certificate[]{cert});
      return keyStore;
    } catch (Exception e) {
      throw new RuntimeException("SSL is enabled but a key store file could not be created. A keystore is required " +
                                   "for SSL to be used.", e);
    }
  }

  /**
   * Creates a new trust store that contains all the public certificates from the given {@link KeyStore}.
   *
   * @param keyStore the {@link KeyStore} for extracting public certificates
   * @return a new instance of {@link KeyStore} that only contains public certificates
   */
  public static KeyStore createTrustStore(KeyStore keyStore) {
    try {
      KeyStore trustStore = KeyStore.getInstance(SSL_KEYSTORE_TYPE);
      trustStore.load(null);

      Enumeration<String> aliases = keyStore.aliases();
      while (aliases.hasMoreElements()) {
        String alias = aliases.nextElement();
        trustStore.setCertificateEntry(alias, keyStore.getCertificate(alias));
      }

      return trustStore;
    } catch (CertificateException | NoSuchAlgorithmException | IOException | KeyStoreException e) {
      throw new RuntimeException("Failed to create trust store from the given key store", e);
    }
  }

  /**
   * Generates a cryptographically strong password
   */
  public static String generateRandomPassword() {
    // This works by choosing 130 bits from a cryptographically secure random bit generator, and encoding them in
    // base-32. 128 bits is considered to be cryptographically strong, but each digit in a base 32 number can encode
    // 5 bits, so 128 is rounded up to the next multiple of 5. Base 32 system uses alphabets A-Z and numbers 2-7
    return new BigInteger(130, new SecureRandom()).toString(32);
  }

  /**
   * Generate an X.509 certificate
   *
   * @param dn Distinguished name for the owner of the certificate, it will also be the signer of the certificate.
   * @param pair Key pair used for signing the certificate.
   * @param days Validity of the certificate.
   * @param algorithm Name of the signature algorithm used.
   * @return A X.509 certificate
   */
  private static X509Certificate getCertificate(String dn, KeyPair pair, int days, String algorithm) throws IOException,
    CertificateException, NoSuchProviderException, NoSuchAlgorithmException, InvalidKeyException, SignatureException {
    // Calculate the validity interval of the certificate
    Date from = new Date();
    Date to = DateUtils.addDays(from, days);
    CertificateValidity interval = new CertificateValidity(from, to);
    // Generate a random number to use as the serial number for the certificate
    BigInteger sn = new BigInteger(64, new SecureRandom());
    // Create the name of the owner based on the provided distinguished name
    X500Name owner = new X500Name(dn);
    // Create an info objects with the provided information, which will be used to create the certificate
    X509CertInfo info = new X509CertInfo();
    info.set(X509CertInfo.VALIDITY, interval);
    info.set(X509CertInfo.SERIAL_NUMBER, new CertificateSerialNumber(sn));
    // In java 7, subject is of type CertificateSubjectName and issuer is of type CertificateIssuerName.
    // These were changed to X500Name in Java8. So looking at the field type before setting them.
    // This certificate will be self signed, hence the subject and the issuer are same.
    Field subjectField = null;
    try {
      subjectField = info.getClass().getDeclaredField("subject");
      if (subjectField.getType().equals(X500Name.class)) {
        info.set(X509CertInfo.SUBJECT, owner);
        info.set(X509CertInfo.ISSUER, owner);
      } else {
        info.set(X509CertInfo.SUBJECT, new CertificateSubjectName(owner));
        info.set(X509CertInfo.ISSUER, new CertificateIssuerName(owner));
      }
    } catch (NoSuchFieldException e) {
      // Trying to set it to Java 8 types. If one of the underlying fields has changed then this will throw a
      // CertificateException which is handled by the caller.
      info.set(X509CertInfo.SUBJECT, owner);
      info.set(X509CertInfo.ISSUER, owner);
    }
    info.set(X509CertInfo.KEY, new CertificateX509Key(pair.getPublic()));
    info.set(X509CertInfo.VERSION, new CertificateVersion(CertificateVersion.V3));
    AlgorithmId algo = new AlgorithmId(AlgorithmId.sha1WithRSAEncryption_oid);
    info.set(X509CertInfo.ALGORITHM_ID, new CertificateAlgorithmId(algo));
    // Create the certificate and sign it with the private key
    X509CertImpl cert = new X509CertImpl(info);
    PrivateKey privateKey = pair.getPrivate();
    cert.sign(privateKey, algorithm);
    return cert;
  }
}
