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

import com.google.common.hash.Hashing;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.twill.filesystem.Location;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.bc.BcPEMDecryptorProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;

/**
 * Utility class with methods for generating a X.509 self signed certificate and creating a Java key
 * store with a self signed certificate.
 */
public final class KeyStores {

  // Default days of validity for certificates generated by this class.
  static final int VALIDITY = 999;

  static final String SSL_KEYSTORE_TYPE = "JKS";

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
  static final String DISTINGUISHED_NAME = "CN=CDAP,L=Palo Alto,C=US";
  static final String SIGNATURE_ALGORITHM = "SHA1withRSA";
  static final String CERT_ALIAS = "cert";
  private static final int KEY_SIZE = 2048;

  /* private constructor */
  private KeyStores() {
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
      SecureRandom random = SecureRandom.getInstance(SECURE_RANDOM_ALGORITHM,
          SECURE_RANDOM_PROVIDER);
      keyGen.initialize(KEY_SIZE, random);
      // generate a key pair
      KeyPair pair = keyGen.generateKeyPair();

      Certificate cert = getCertificate(DISTINGUISHED_NAME, pair, validityDays,
          SIGNATURE_ALGORITHM);

      KeyStore keyStore = KeyStore.getInstance(SSL_KEYSTORE_TYPE);
      keyStore.load(null, password.toCharArray());
      keyStore.setKeyEntry(CERT_ALIAS, pair.getPrivate(), password.toCharArray(),
          new java.security.cert.Certificate[]{cert});
      return keyStore;
    } catch (Exception e) {
      throw new RuntimeException("Failed to generate a new keystore with self signed certificate.",
          e);
    }
  }

  /**
   * Creates a key store from the given PEM file.
   *
   * @param certificatePath the file path to the PEM file
   * @param password password for decrypting keypair.
   * @return a {@link KeyStore} with certificates and keys as provided by the given file
   * @throws RuntimeException if failed to create the keystore.
   */
  public static KeyStore createKeyStore(Path certificatePath, String password) {
    PrivateKey privateKey = null;
    List<Certificate> certificates = new ArrayList<>();

    BouncyCastleProvider provider = new BouncyCastleProvider();
    JcaPEMKeyConverter keyConverter = new JcaPEMKeyConverter().setProvider(provider);
    JcaX509CertificateConverter certConverter = new JcaX509CertificateConverter().setProvider(
        provider);
    char[] passPhase = password.toCharArray();

    try (PEMParser parser = new PEMParser(
        Files.newBufferedReader(certificatePath, StandardCharsets.ISO_8859_1))) {
      Object obj = parser.readObject();
      while (obj != null) {
        // Decrypt the key block if it is encrypted
        if (obj instanceof PEMEncryptedKeyPair) {
          obj = ((PEMEncryptedKeyPair) obj).decryptKeyPair(new BcPEMDecryptorProvider(passPhase));
        }

        // Set the private key if it is not set
        if (obj instanceof PEMKeyPair && privateKey == null) {
          privateKey = keyConverter.getKeyPair((PEMKeyPair) obj).getPrivate();
        } else if (obj instanceof X509CertificateHolder) {
          // Add the cert to the cert chain
          certificates.add(certConverter.getCertificate((X509CertificateHolder) obj));
        }

        obj = parser.readObject();
      }

      if (privateKey == null) {
        throw new RuntimeException("Missing private key from file " + certificatePath);
      }

      KeyStore keyStore = KeyStore.getInstance(SSL_KEYSTORE_TYPE);
      keyStore.load(null, passPhase);
      keyStore.setKeyEntry(CERT_ALIAS, privateKey, passPhase,
          certificates.toArray(new Certificate[0]));
      return keyStore;
    } catch (IOException | CertificateException | NoSuchAlgorithmException | KeyStoreException e) {
      throw new RuntimeException("Failed to create keystore from PEM file " + certificatePath, e);
    }
  }

  /**
   * Creates a new trust store that contains all the public certificates from the given {@link
   * KeyStore}.
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
   * Loads a {@link KeyStore} from the given {@link Location} with keystore type {@link
   * KeyStores#SSL_KEYSTORE_TYPE}.
   *
   * @param location the {@link Location} to read the serialized {@link KeyStore}
   * @return a new instance of {@link KeyStore}.
   * @throws IOException if failed to read from the given {@link Location}.
   * @throws GeneralSecurityException if failed to construct the {@link KeyStore}.
   */
  public static KeyStore load(Location location,
      Supplier<String> keystorePassSupplier) throws IOException, GeneralSecurityException {
    KeyStore ks = KeyStore.getInstance(KeyStores.SSL_KEYSTORE_TYPE);
    try (InputStream is = location.getInputStream()) {
      ks.load(is, keystorePassSupplier.get().toCharArray());
    }
    return ks;
  }

  /**
   * Computes a secure hash from the given {@link KeyStore} content.
   */
  public static String hash(KeyStore keyStore) throws IOException, GeneralSecurityException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    keyStore.store(bos, "".toCharArray());
    return Hashing.sha512().hashBytes(bos.toByteArray()).toString();
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
   * @param dn Distinguished name for the owner of the certificate, it will also be the signer
   *     of the certificate.
   * @param pair Key pair used for signing the certificate.
   * @param days Validity of the certificate.
   * @param algorithm Name of the signature algorithm used.
   * @return A X.509 certificate
   */
  private static Certificate getCertificate(String dn, KeyPair pair, int days, String algorithm)
      throws IOException, OperatorCreationException, CertificateException {

    BouncyCastleProvider provider = new BouncyCastleProvider();

    // Calculate the validity interval of the certificate
    Date from = new Date();
    Date to = new Date(from.getTime() + TimeUnit.DAYS.toMillis(days));

    // Generate a random number to use as the serial number for the certificate
    BigInteger sn = new BigInteger(64, new SecureRandom());
    // Create the name of the owner based on the provided distinguished name
    X500Name owner = new X500Name(dn);
    // Create an info objects with the provided information, which will be used to create the certificate

    SubjectPublicKeyInfo publicKeyInfo = SubjectPublicKeyInfo.getInstance(
        pair.getPublic().getEncoded());
    AsymmetricKeyParameter privateKeyParam = PrivateKeyFactory.createKey(
        pair.getPrivate().getEncoded());
    AlgorithmIdentifier sigAlgId = new DefaultSignatureAlgorithmIdentifierFinder().find(algorithm);
    AlgorithmIdentifier digAlgId = new DefaultDigestAlgorithmIdentifierFinder().find(sigAlgId);
    ContentSigner signer = new BcRSAContentSignerBuilder(sigAlgId, digAlgId).build(privateKeyParam);

    X509CertificateHolder certHolder = new X509v3CertificateBuilder(owner, sn, from, to,
        owner, publicKeyInfo).build(signer);
    return new JcaX509CertificateConverter().setProvider(provider).getCertificate(certHolder);
  }
}
