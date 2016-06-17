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
 * the License.
 */

package co.cask.cdap.security.securestore;

import co.cask.cdap.api.security.SecureStoreMetadata;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Configuration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.security.SecureStoreProvider;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import javax.crypto.spec.SecretKeySpec;

/**
 * File based implementation of secure store. Uses Java JCEKS based keystore.
 */
@Singleton
public class FileSecureStoreProvider implements SecureStoreProvider {
  private static final Logger LOG = LoggerFactory.getLogger(FileSecureStoreProvider.class);

  private static final String SCHEME_NAME = "jceks";
  /* This is used to create the key for storing metadata in the keystore. Changing this will make the
     metadata for existing entries unreachable. */
  private static final String METADATA_SUFFIX = "_metadata";
  private static final String SECURE_STORE_DEFAULT_FILE_PATH = "/tmp";
  private static final String SECURE_STORE_DEFAULT_FILE_NAME = "securestore";
  private static final char[] SECURE_STORE_DEFAULT_PASSWORD = "cdapsecret".toCharArray();
  /* Java Keystore needs an algorithm name to store a key, it is not used for any checks, only stored,
     since we are not handling the encryption we don't care about this. */
  private static final String ALGORITHM_PROXY = "none";

  /* A cache for metadata.
     The assumption is that the number of entries won't be large enough to worry about memory. */
  private final Map<String, SecureStoreMetadata> cache = new HashMap<>();
  private final char[] password;
  private final Path path;
  private final Lock readLock;
  private final Lock writeLock;
  private final CConfiguration cConf;

  private KeyStore keyStore;
  private boolean changed = false;

  private FileSecureStoreProvider(CConfiguration cConf) {
    this.cConf = cConf;
    // Get the path to the keystore file
    String pathString = cConf.get(Constants.Security.Store.SECURE_STORE_FILE_PATH);
    if (pathString == null || pathString.isEmpty()) {
      pathString = SECURE_STORE_DEFAULT_FILE_PATH;
    }
    Path dir = Paths.get(pathString);
    path = dir.resolve(SECURE_STORE_DEFAULT_FILE_NAME);

    // Get the keystore password
    String passwordString = cConf.get(Constants.Security.Store.SECURE_STORE_FILE_PASSWORD);
    if (passwordString == null || passwordString.isEmpty()) {
      password = SECURE_STORE_DEFAULT_PASSWORD;
    } else {
      password = passwordString.toCharArray();
    }

    ReadWriteLock lock = new ReentrantReadWriteLock(true);
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  public static FileSecureStoreProvider getInstance(CConfiguration cConf) throws IOException {
    FileSecureStoreProvider fileSecureStoreProvider = new FileSecureStoreProvider(cConf);
    // Locate and load the keystore
    fileSecureStoreProvider.locateKeystore();
    return fileSecureStoreProvider;
  }

  void put(String name, byte[] data, Map<String, String> properties) throws IOException {
    String metaKey = constructMetadataKey(name);
    writeLock.lock();
    try {
      try {
        if (keyStore.containsAlias(name) || cache.containsKey(metaKey)) {
          // Clear the existing key so that we can write the new one.
          delete(name);
        }
        cache.put(metaKey, FileSecureStoreMetadata.of(name, properties));
        keyStore.setKeyEntry(name, new SecretKeySpec(data, ALGORITHM_PROXY),
                             password, null);
      } catch (KeyStoreException e) {
        cache.remove(metaKey);
        throw new IOException("Failed to store the key. ", e);
      }
    } finally {
      writeLock.unlock();
    }
    // Attempt to persist the store.
    flush();
  }

  void delete(String name) throws IOException {
    String metaKey = constructMetadataKey(name);
    writeLock.lock();
    try {
      if (cache.containsKey(metaKey)) {
        cache.remove(metaKey);
      }
      if (keyStore.containsAlias(name)) {
        keyStore.deleteEntry(name);
      }
    } catch (KeyStoreException e) {
      LOG.warn("Unable to delete the key " + name, e);
    } finally {
      writeLock.unlock();
    }
    // Attempt to persist the store.
    flush();
  }

  Map<String, String> list() throws IOException {
    Map<String, String> list = new HashMap<>();
    readLock.lock();
    try {
      String alias = null;
      try {
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
          alias = aliases.nextElement();
          list.put(alias, getDescription(alias));
        }
      } catch (KeyStoreException e) {
        throw new IOException("Can't get key " + alias + " from " + path, e);
      }
      return list;

    } finally {
      readLock.unlock();
    }
  }

  @Nullable
  SecureStoreMetadata getSecureStoreMetadata(String name) throws KeyStoreException {
    String metaKey = constructMetadataKey(name);
    if (cache.containsKey(metaKey)) {
      return cache.get(metaKey);
    } else if (keyStore.containsAlias(metaKey)) {
      try {
        SecureStoreMetadata meta = ((KeyMetadata) keyStore.getKey(metaKey, password)).metadata;
        cache.put(metaKey, meta);
        return meta;
      } catch (NoSuchAlgorithmException | UnrecoverableKeyException e) {
        e.printStackTrace();
      }
    }
    return null;
  }

  byte[] getData(String name) throws IOException {
    try {
      if (!keyStore.containsAlias(name)) {
        throw new IOException(name + " not found in the keystore.");
      }
      SecretKeySpec key = (SecretKeySpec) keyStore.getKey(name, password);
      return key.getEncoded();
    } catch (NoSuchAlgorithmException | UnrecoverableKeyException | KeyStoreException e) {
      throw new IOException("Unable to retrieve the key " + name);
    }
  }

  /**
   * @return the provider configuration
   */
  Configuration getcConf() {
    return cConf;
  }

  private static Path constructOldPath(Path path) {
    return Paths.get(path.toString(), "_OLD");
  }

  private static Path constructNewPath(Path path) {
    return Paths.get(path.toString(), "_NEW");
  }

  private static void loadFromPath(KeyStore keyStore, Path path, char[] password)
    throws IOException {
    try (InputStream in = new DataInputStream(Files.newInputStream(path))) {
      keyStore.load(in, password);
    } catch (NoSuchAlgorithmException | CertificateException e) {
      throw new IOException("Unable to load the Secure Store. ", e);
    }
  }

  private static boolean isBadOrWrongPassword(IOException ioe) {
    /* According to Java keystore documentation if the load failed due to bad password
       then the cause of the exception would be set to "UnrecoverableKeyException".
       Unfortunately that is not the observed behavior. */
    if (ioe.getCause() instanceof UnrecoverableKeyException) {
      return true;
    }
    // Workaround
    return (ioe.getCause() == null)
      && (ioe.getMessage() != null)
      && ((ioe.getMessage().contains("Keystore was tampered")) || (ioe
      .getMessage().contains("password was incorrect")));
  }

  private static void renameOrFail(Path src, Path dest) throws IOException {
    Files.move(src, src.resolveSibling(dest));
  }

  private static String constructMetadataKey(String name) {
    return name + METADATA_SUFFIX;
  }

  /**
   * Initialize the keyStore.
   *
   * @throws IOException If there is a problem reading or creating the keystore.
   */
  private void locateKeystore() throws IOException {
    Path oldPath = constructOldPath(path);
    Path newPath = constructNewPath(path);
    try {
      keyStore = KeyStore.getInstance(SCHEME_NAME);
      if (Files.exists(path)) {
        // If the main file exists then the new path should not exist.
        // Both existing means there is an inconsistency.
        if (Files.exists(newPath)) {
          throw new IOException(
            String.format("Secure Store not loaded due to an inconsistency "
                            + "('%s' and '%s' should not exist together)!!", path, newPath));
        }
        tryLoadFromPath(path, oldPath);
      } else if (!tryLoadIncompleteFlush(newPath, oldPath)) {
        // We were not able to load an existing key store. Create a new one.
        keyStore.load(null, password);
        LOG.info("New Secure Store initialized successfully.");
      }
    } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException e) {
      LOG.error("Can't create Secure Store. " + e);
    }
  }

  /**
   * Try loading from the user specified path, if that fails for any reason
   * other than bad password then try loading from the backup path.
   *
   * @param path       Path to load from
   * @param backupPath Backup path (_OLD)
   * @throws IOException
   */
  private void tryLoadFromPath(Path path, Path backupPath) throws IOException {
    try {
      loadFromPath(keyStore, path, password);
      // Successfully loaded the keystore. No need to keep the old file.
      Files.deleteIfExists(backupPath);
      LOG.info("Secure store loaded successfully.");
    } catch (IOException ioe) {
      // Try the backup path if the loading failed for any reason other than incorrect password.
      if (!isBadOrWrongPassword(ioe)) {
        // Mark the current file as CORRUPTED
        renameOrFail(path, Paths.get(path.toString() + "_CORRUPTED_" + System.currentTimeMillis()));
        // Try loading from the backup path
        loadFromPath(keyStore, backupPath, password);
        renameOrFail(backupPath, path);
        LOG.warn(String.format("Secure store loaded successfully from '%s' since '%s'" + " was corrupted.",
                               backupPath, path));
      } else {
        // Failed due to bad password.
        throw ioe;
      }
    }
  }

  /**
   * The KeyStore might have gone down during a flush, In which case either the
   * _NEW or _OLD files might exists. This method tries to load the KeyStore
   * from one of these intermediate files.
   * @param oldPath the _OLD file created during flush
   * @param newPath the _NEW file created during flush
   * @return If the file was successfully loaded
   */
  private boolean tryLoadIncompleteFlush(Path oldPath, Path newPath)
    throws IOException {
    // Check if _NEW exists (in case flush had finished writing but not
    // completed the re-naming)
    boolean loaded = false;
    if (Files.exists(oldPath)) {
      loadFromPath(keyStore, oldPath, password);
      loaded = true;
      // Successfully loaded from the old file, rename it.
      renameOrFail(oldPath, path);
    }
    if (!loaded && Files.exists(newPath)) {
      loadFromPath(keyStore, newPath, password);
      loaded = true;
      // Successfully loaded from the new file, rename it.
      renameOrFail(newPath, path);
    }
    return loaded;
  }

  @Nullable
  private String getDescription(String name) throws KeyStoreException {
    SecureStoreMetadata meta = getSecureStoreMetadata(name);
    return meta == null ? null : meta.getDescription();
  }

  private void flush() throws IOException {
    Path newPath = constructNewPath(path);
    Path oldPath = constructOldPath(path);
    Path resetPath = path;
    writeLock.lock();
    try {
      if (!changed) {
        return;
      }
      // Might exist if a backup has been restored etc.
      if (Files.exists(newPath)) {
        renameOrFail(newPath, Paths.get(newPath.toString() + "_ORPHANED_" + System.currentTimeMillis()));
      }
      if (Files.exists(oldPath)) {
        renameOrFail(oldPath, Paths.get(oldPath.toString() + "_ORPHANED_" + System.currentTimeMillis()));
      }
      // put all of the updates into the keystore
      for (Map.Entry<String, SecureStoreMetadata> entry: cache.entrySet()) {
        try {
          keyStore.setKeyEntry(entry.getKey(), new KeyMetadata(entry.getValue()),
                               password, null);
        } catch (KeyStoreException e) {
          throw new IOException("Can't set metadata key " + entry.getKey(), e);
        }
      }

      // Create the backup copy
      boolean fileExisted = backupToOld(oldPath);
      if (fileExisted) {
        resetPath = oldPath;
      }
      // Flush the keystore, write the _NEW file first
      try {
        writeToNew(newPath);
      } catch (IOException ioe) {
        // rename _OLD back to current and throw Exception
        revertFromOld(oldPath, fileExisted);
        resetPath = path;
        throw ioe;
      }
      // Rename _NEW to CURRENT and delete _OLD
      cleanupNewAndOld(newPath, oldPath);
      changed = false;
    } catch (IOException ioe) {
      resetKeyStoreState(resetPath);
      throw ioe;
    } finally {
      writeLock.unlock();
    }
  }

  private boolean backupToOld(Path oldPath) throws IOException {
    boolean fileExisted = false;
    if (Files.exists(path)) {
      renameOrFail(path, oldPath);
      fileExisted = true;
    }
    return fileExisted;
  }

  private void resetKeyStoreState(Path path) {
    LOG.debug("Could not flush Keystore.."
                + "attempting to reset to previous state !!");
    // 1) flush cache
    cache.clear();
    // 2) load keyStore from previous path
    try {
      loadFromPath(keyStore, path, password);
      LOG.debug("KeyStore resetting to previously flushed state !!");
    } catch (Exception e) {
      LOG.debug("Could not reset Keystore to previous state", e);
    }
  }

  private void cleanupNewAndOld(Path newPath, Path oldPath) throws IOException {
    // Rename _NEW to CURRENT
    renameOrFail(newPath, path);
    // Delete _OLD
    if (Files.exists(oldPath)) {
      Files.delete(oldPath);
    }
  }

  private void writeToNew(Path newPath) throws IOException {
    try (OutputStream fos = new DataOutputStream(Files.newOutputStream(newPath))) {
      keyStore.store(fos, password);
    } catch (KeyStoreException e) {
      throw new IOException("Can't store keystore " + this, e);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(
        "No such algorithm storing keystore " + this, e);
    } catch (CertificateException e) {
      throw new IOException(
        "Certificate exception storing keystore " + this, e);
    }
  }

  private void revertFromOld(Path oldPath, boolean fileExisted)
    throws IOException {
    if (fileExisted) {
      renameOrFail(oldPath, path);
    }
  }

  /**
   * An adapter between a KeyStore Key and our SecureStoreMetadata. This is used to store
   * the metadata in a KeyStore even though isn't really a key.
   */
  private static class KeyMetadata implements Key, Serializable {
    private SecureStoreMetadata metadata;
    private static final long serialVersionUID = 3405839418917868651L;
    private static final String METADATA_FORMAT = "KeyMetadata";

    private KeyMetadata(SecureStoreMetadata meta) {
      this.metadata = meta;
    }

    @Override
    public String getAlgorithm() {
      return ALGORITHM_PROXY;
    }

    @Override
    public String getFormat() {
      return METADATA_FORMAT;
    }

    @Override
    public byte[] getEncoded() {
      return new byte[0];
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      byte[] serialized = ((FileSecureStoreMetadata) metadata).serialize();
      out.writeInt(serialized.length);
      out.write(serialized);
    }

    private void readObject(ObjectInputStream in
    ) throws IOException, ClassNotFoundException {
      byte[] buf = new byte[in.readInt()];
      in.readFully(buf);
      metadata = new FileSecureStoreMetadata(buf);
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
//    char[] password = "none".toCharArray();
//    char[] wrongPassword = "wrong".toCharArray();
//    KeyStore ks = null;
//    FileOutputStream fos;
//    String alias = "First";
//    try {
//      ks = KeyStore.getInstance(SCHEME_NAME);
//      ks.load(null, password);
//      ks.setKeyEntry(alias, new SecretKeySpec("Super secret".getBytes(), "RSA"),
//          password, null);
//      System.out.println(ks.containsAlias(alias));
//      fos = new java.io.FileOutputStream("keyStoreName");
//      ks.store(fos, password);
//      fos.close();
//      ks = null;
//      Thread.sleep(1000);
//      KeyStore ks1 = KeyStore.getInstance(SCHEME_NAME);
//      FileInputStream fis = new FileInputStream("keyStoreName");
//      try {
//        ks1.load(fis, null);
//        System.out.println(ks1.containsAlias(alias));
//      } catch (IOException ioe) {
//        System.out.println("Wrong password");
//        ioe.printStackTrace();
//        System.out.println(ioe.getMessage());
//      }
//      KeyStore ks2 = KeyStore.getInstance(SCHEME_NAME);
//      ks2.load(null, password);
//      System.out.println(ks2.size());
//    } catch (FileNotFoundException | CertificateException | NoSuchAlgorithmException | KeyStoreException e) {
//      e.printStackTrace();
//    }
  }
}
