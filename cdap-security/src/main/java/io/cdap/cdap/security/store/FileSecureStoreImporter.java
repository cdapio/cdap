/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.security.store;

import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.security.store.file.FileSecureStoreCodec;
import io.cdap.cdap.security.store.file.KeyInfo;
import io.cdap.cdap.security.store.file.SecureStoreDataCodecV1;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Enumeration;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service responsible for importing/upgrading the {@link java.security.KeyStore} files saved by the
 * {@link FileSecureStoreService}.
 */
public class FileSecureStoreImporter {

  private static final Logger LOG = LoggerFactory.getLogger(FileSecureStoreImporter.class);
  private static final Class<? extends FileSecureStoreCodec>[] HISTORICAL_CODECS = new Class[]{
      SecureStoreDataCodecV1.class
  };

  private final FileSecureStoreCodec currentCodec;

  public FileSecureStoreImporter(FileSecureStoreCodec currentCodec) {
    this.currentCodec = currentCodec;
  }

  /**
   * Attempts to import a prior KeyStore file from the specified path.
   *
   * @param keyStore The keystore to import into
   * @param path The filepath of the prior keystore
   * @param password The password to use
   * @throws IOException If the import failed
   * @throws IllegalArgumentException If an invalid argument was specified
   */
  public void importFromPath(KeyStore keyStore, Path path, char[] password) throws IOException,
      IllegalArgumentException {
    try (InputStream in = new DataInputStream(Files.newInputStream(path))) {
      keyStore.load(in, password);
    } catch (Exception e) {
      attemptUpgrade(keyStore, path, password);
    }
  }

  private void attemptUpgrade(KeyStore keyStore, Path path, char[] password) throws IOException,
      IllegalArgumentException {
    // Instantiate a new KeyStore.
    try {
      keyStore.load(null, password);
    } catch (NoSuchAlgorithmException | CertificateException e) {
      throw new IOException("Failed to initialize empty keystore for upgrade:", e);
    }

    KeyStore historicalKeyStore = null;
    // KeyStore files using older schemas may fail to import. In these cases, we attempt to upgrade the schema.
    for (Class<? extends FileSecureStoreCodec> historicalCodecClass : HISTORICAL_CODECS) {
      FileSecureStoreCodec historicalCodec;
      // Instantiate an instance of the historical codec.
      try {
        historicalCodec = historicalCodecClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IllegalArgumentException("Failed to instantiate historical codec:", e);
      }

      // Instantiate an instance of the historical KeyStore.
      try {
        historicalKeyStore = KeyStore.getInstance(historicalCodec.getKeyStoreScheme());
      } catch (KeyStoreException e) {
        throw new IllegalArgumentException("Failed to get historical keystore instance:", e);
      }

      try {
        // Load the historical KeyStore.
        try (InputStream in = new DataInputStream(Files.newInputStream(path))) {
          historicalKeyStore.load(in, password);
        } catch (NoSuchAlgorithmException | CertificateException e) {
          LOG.warn("Failed to load historical keystore:", e);
          continue;
        }

        // Iterate over all aliases in the keystore, deserialize them using the historical codec, re-serialize them
        // using the current codec, and import them to the new KeyStore.
        Enumeration<String> historicalAliases;
        try {
          historicalAliases = historicalKeyStore.aliases();
        } catch (KeyStoreException e) {
          throw new IOException("Failed to load historical keys:", e);
        }
        while (historicalAliases.hasMoreElements()) {
          String historicalAlias = historicalAliases.nextElement();
          upgradeKey(keyStore, historicalKeyStore, historicalAlias, historicalCodec, password);
        }
      } catch (Exception e) {
        // If we hit an exception with a particular codec, we continue to try with other codecs.
        LOG.warn("Failed to upgrade using historical codec {}", historicalCodecClass.getName(), e);
      }

      // If we get here, the historical keystore load was successful, so break out of the loop.
      return;
    }

    // If we get here, no compatible historical codec was found.
    throw new IOException("Failed to upgrade KeyStore file using known historical codecs");
  }

  /**
   * Upgrades the key by reading the key from the historical keystore, deserializing the historical
   * key using the historical codec, re-serializing it using the current codec, and adding it to the
   * current keystore.
   *
   * @param currentKeyStore The current keystore
   * @param historicalKeyStore The historical keystore
   * @param historicalAlias The alias for the historical key
   * @param historicalCodec The historical codec
   * @param password The password to use for both keystores
   * @throws IOException If upgrading the key fails
   */
  private void upgradeKey(KeyStore currentKeyStore, KeyStore historicalKeyStore,
      String historicalAlias,
      FileSecureStoreCodec historicalCodec, char[] password) throws IOException {

    KeyInfo historicalKeyInfo = historicalCodec.getKeyInfoFromAlias(historicalAlias);
    Key historicalKey;
    try {
      historicalKey = historicalKeyStore.getKey(historicalAlias, password);
    } catch (KeyStoreException e) {
      throw new IOException(
          String.format("Failed to load historical key alias '%s':", historicalAlias), e);
    } catch (NoSuchAlgorithmException | UnrecoverableKeyException e) {
      // These exceptions indicate a single key load failure, so we skip instead of failing the entire import.
      LOG.warn("Failed to import historical key alias '{}':", historicalAlias, e);
      return;
    }
    SecureStoreData historicalData = historicalCodec.decode(historicalKey.getEncoded());
    try {
      SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(FileSecureStoreService
          .SECRET_KEY_FACTORY_ALGORITHM);
      // Convert byte[] directly to char[] and avoid using String due
      // to it being stored in memory until garbage collected.
      PBEKeySpec pbeKeySpec = new PBEKeySpec(StandardCharsets.UTF_8
          .decode(ByteBuffer.wrap(currentCodec.encode(historicalData)))
          .array());
      currentKeyStore.setKeyEntry(currentCodec.getKeyAliasFromInfo(historicalKeyInfo),
          secretKeyFactory.generateSecret(pbeKeySpec), password, null);
    } catch (KeyStoreException | NoSuchAlgorithmException e) {
      throw new IllegalArgumentException(
          String.format("Failed to import secret key alias '%s':", historicalAlias),
          e);
    } catch (InvalidKeySpecException e) {
      LOG.warn("Failed to generate PBE key", e);
    }
  }
}
