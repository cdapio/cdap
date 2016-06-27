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

package co.cask.cdap.security.securestore;

import co.cask.cdap.api.security.securestore.SecureStore;
import co.cask.cdap.api.security.securestore.SecureStoreData;
import co.cask.cdap.api.security.securestore.SecureStoreManager;
import co.cask.cdap.api.security.securestore.SecureStoreMetadata;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
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
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.crypto.spec.SecretKeySpec;

/**
 * File based implementation of secure store. Uses Java JCEKS based keystore.
 * The data and its metadata are both stored in the keystore separately.
 * The data is stored with the provided name as the key and the metadata is
 * stored with the provided name + "_metadata" as the key. They are stored separately
 * because they are accessed separately most of the time.
 *
 * When the client calls a put, the key is put in the keystore, the metadata object
 * for that key is created and that is put in the store too. The system then flushes
 * the keystore to the file system.
 * During the flush, the current file is first backed up (_OLD). Then the keystore
 * is written to temporary file (_NEW). If that is successful then the temporary
 * file is renamed to the secure store file. If anything fails during this process
 * then the keystore reverts to the last successfully written file.
 *
 *  The keystore is flushed to the filesystem after every put and delete.
 */
class FileSecureStore implements SecureStore, SecureStoreManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileSecureStore.class);

  private static final String SCHEME_NAME = "jceks";
  /*
   This is used to create the key for storing metadata in the keystore. Changing this will make the
   metadata for existing entries unreachable.
   */
  private static final String METADATA_SUFFIX = "_metadata";
  /*
   Java Keystore needs an algorithm name to store a key, it is not used for any checks, only stored,
   since we are not handling the encryption we don't care about this.
  */
  private static final String ALGORITHM_PROXY = "none";

  private final char[] password;
  private final Path path;
  private final Lock readLock;
  private final Lock writeLock;
  private final KeyStore keyStore;
  private boolean changed = false;

  FileSecureStore(CConfiguration cConf) throws IOException {
    // Get the path to the keystore file
    String pathString = cConf.get(Constants.Security.Store.FILE_PATH);
    Path dir = Paths.get(pathString);
    path = dir.resolve(Constants.Security.Store.FILE_NAME);

    // Get the keystore password
    password = cConf.get(Constants.Security.Store.FILE_PASSWORD).toCharArray();

    keyStore = locateKeystore(path, password);
    ReadWriteLock lock = new ReentrantReadWriteLock(true);
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  /**
   * Stores an element in the secure store. If the key already exists then delete it first.
   * @param name Name of the element to store
   * @param data The data that needs to be securely stored
   * @param properties Metadata associated with the data
   * @throws IOException
   */
  @Override
  public void put(String name, byte[] data, Map<String, String> properties) throws IOException {
    String metaKey = constructMetadataKey(name);
    writeLock.lock();
    try {
      if (keyStore.containsAlias(name)) {
        // Clear the existing key so that we can write the new one.
        delete(name);
      }
      keyStore.setKeyEntry(name, new SecretKeySpec(data, ALGORITHM_PROXY),
                           password, null);
      SecureStoreMetadata meta = SecureStoreMetadata.of(name, properties);
      /*
        The data was written successfully to the key store. Now try to write the metddata.
        If this fails then try to delete the data and throw an exception.
       */
      try {
        keyStore.setKeyEntry(metaKey, new KeyMetadata(meta), password, null);
      } catch (KeyStoreException k) {
        keyStore.deleteEntry(name);
        throw k;
      }
      // Attempt to persist the store. If that fails then remove the element and rethrow the Exception.
      flush();
    } catch (KeyStoreException e) {
      throw new IOException("Failed to store the key. ", e);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Deletes the element with the given name.
   * @param name Name of the element to be deleted
   */
  @Override
  public void delete(String name) throws IOException {
    String metaKey = constructMetadataKey(name);
    writeLock.lock();
    try {
      if (keyStore.containsAlias(name)) {
        keyStore.deleteEntry(name);
      }
      /* It's OK if we were able to delete the data but not the metadata.
         Java keystore allows overwrites, so the next time we try to write a
         key with the same name the metadata will be overwritten. */
      if (keyStore.containsAlias(metaKey)) {
        keyStore.deleteEntry(metaKey);
      }
      // Attempt to persist the store. If that fails then remove the element and rethrow the Exception.
      flush();
    } catch (KeyStoreException e) {
      LOG.error("Failed to delete the key " + name, e);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * List of all the entries in the secure store.
   * @return A list of {@link SecureStoreMetadata} objects representing the data stored in the store.
   * @throws IOException
   */
  @Override
  public List<SecureStoreMetadata> list() throws IOException {
    List<SecureStoreMetadata> list = new ArrayList<>();
    String name;
    readLock.lock();
    try {
      try {
        Enumeration<String> aliases = keyStore.aliases();
        // Surprisingly enhanced for-each does not work for Enumeration
        while (aliases.hasMoreElements()) {
          name = aliases.nextElement();
          // We don't want to list the meta data elements separately.
          if (name.endsWith(METADATA_SUFFIX)) {
            continue;
          }
          list.add(getSecureStoreMetadata(name));
        }
      } catch (KeyStoreException e) {
        throw new IOException("Failed to get the list of elements from the secure store.", e);
      }
      return list;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * @param name Name of the data element.
   * @return An object representing the securely stored data associated with the name.
   */
  @Override
  public SecureStoreData get(String name) throws IOException {
    return new SecureStoreData(getSecureStoreMetadata(name), getData(name));
  }

  /**
   * Returns the metadata for the element identified by the given name.
   * @param name Name of the element
   * @return An object representing the metadata associated with the element
   * @throws IOException
   */
  private SecureStoreMetadata getSecureStoreMetadata(String name) throws IOException {
    String metaKey = constructMetadataKey(name);
    readLock.lock();
    try {
      if (!keyStore.containsAlias(metaKey)) {
        throw new IOException("Metadata for " + name + " not found in the secure store.");
      }
      Key key = keyStore.getKey(metaKey, password);
      return ((KeyMetadata) key).metadata;
    } catch (NoSuchAlgorithmException | UnrecoverableKeyException | KeyStoreException e) {
      throw new IOException("Unable to retrieve the metadata for " + name, e);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns the securely stored data as a UTF8 encoded byte array.
   * @param name Name of the element in the secure store
   * @return The data associated with the element as an UTF8 formatted byte array
   * @throws IOException
   */
  private byte[] getData(String name) throws IOException {
    readLock.lock();
    try {
      if (!keyStore.containsAlias(name)) {
        throw new IOException(name + " not found in the secure store.");
      }
      SecretKeySpec key = (SecretKeySpec) keyStore.getKey(name, password);
      return key.getEncoded();
    } catch (NoSuchAlgorithmException | UnrecoverableKeyException | KeyStoreException e) {
      throw new IOException("Unable to retrieve the key " + name, e);
    } finally {
      readLock.unlock();
    }
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

  private static String constructMetadataKey(String name) {
    return name + METADATA_SUFFIX;
  }

  /**
   * Initialize the keyStore.
   *
   * @throws IOException If there is a problem reading or creating the keystore.
   */
  private static KeyStore locateKeystore(Path path, final char[] password) throws IOException {
    Path oldPath = constructOldPath(path);
    Path newPath = constructNewPath(path);
    KeyStore ks;
    try {
      ks = KeyStore.getInstance(SCHEME_NAME);
      if (Files.exists(path)) {
        // If the main file exists then the new path should not exist.
        // Both existing means there is an inconsistency.
        if (Files.exists(newPath)) {
          throw new IOException(
            String.format("Secure Store not loaded due to an inconsistency "
                            + "('%s' and '%s' should not exist together)!!", path, newPath));
        }
        tryLoadFromPath(ks, path, oldPath, password);
      } else if (!tryLoadIncompleteFlush(ks, path, newPath, oldPath, password)) {
        // We were not able to load an existing key store. Create a new one.
        ks.load(null, password);
        LOG.info("New Secure Store initialized successfully.");
      }
    } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException e) {
      throw new IOException("Can't create Secure Store. ", e);
    }
    return ks;
  }

  /**
   * Try loading from the user specified path, if that fails for any reason
   * other than bad password then try loading from the backup path.
   *
   * @param path       Path to load from
   * @param backupPath Backup path (_OLD)
   * @throws IOException
   */
  private static void tryLoadFromPath(KeyStore keyStore, Path path, Path backupPath, char[] password) throws IOException {
    try {
      loadFromPath(keyStore, path, password);
      // Successfully loaded the keystore. No need to keep the old file.
      Files.deleteIfExists(backupPath);
      LOG.info("Secure store loaded successfully.");
    } catch (IOException ioe) {
      // Try the backup path if the loading failed for any reason other than incorrect password.
      if (!isBadOrWrongPassword(ioe)) {
        // Mark the current file as CORRUPTED
        Files.move(path, Paths.get(path.toString() + "_CORRUPTED_" + System.currentTimeMillis()));
        // Try loading from the backup path
        loadFromPath(keyStore, backupPath, password);
        Files.move(backupPath, path);
        LOG.warn("Secure store loaded successfully from " + backupPath + " since " + path + " was corrupted.");
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
  private static boolean tryLoadIncompleteFlush(KeyStore keyStore, Path path, Path oldPath, Path newPath,
                                                final char[] password)
    throws IOException {
    // Check if _NEW exists (in case flush had finished writing but not
    // completed the re-naming)
    boolean loaded = false;
    if (Files.exists(oldPath)) {
      loadFromPath(keyStore, oldPath, password);
      loaded = true;
      // Successfully loaded from the old file, rename it.
      Files.move(oldPath, path);
    }
    if (!loaded && Files.exists(newPath)) {
      loadFromPath(keyStore, newPath, password);
      loaded = true;
      // Successfully loaded from the new file, rename it.
      Files.move(newPath, path);
    }
    return loaded;
  }

  /**
   * Persist the keystore on the file system.
   * First save the current file as a backup, then store the current data in a new file.
   * If all goes well then renme the new file to current and delete the old file.
   * If anything fails then try to back up from the last successfully written file.
   *
   * @throws IOException
   */
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
        Files.move(newPath, Paths.get(newPath.toString() + "_ORPHANED_" + System.currentTimeMillis()));
      }
      if (Files.exists(oldPath)) {
        Files.move(oldPath, Paths.get(oldPath.toString() + "_ORPHANED_" + System.currentTimeMillis()));
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
      LOG.error("Failed to persist the key store. Secure data may be lost on a restart.", ioe);
      throw ioe;
    } finally {
      writeLock.unlock();
    }
  }

  private boolean backupToOld(Path oldPath) throws IOException {
    boolean fileExisted = false;
    if (Files.exists(path)) {
      Files.move(path, oldPath);
      fileExisted = true;
    }
    return fileExisted;
  }

  private void resetKeyStoreState(Path path) {
    LOG.debug("Could not flush Keystore attempting to reset to previous state.");
    // load keyStore from previous path
    try {
      loadFromPath(keyStore, path, password);
      LOG.debug("KeyStore resetting to previously flushed state.");
    } catch (Exception e) {
      LOG.debug("Could not reset Keystore to previous state.", e);
    }
  }

  private void cleanupNewAndOld(Path newPath, Path oldPath) throws IOException {
    // Rename _NEW to CURRENT
    Files.move(newPath, path);
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
      Files.move(oldPath, path);
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
    // This method is never called. It is here to satisfy the Key interface. We need to implement the key interface
    // so that we can store the metadata in the keystore.
    public byte[] getEncoded() {
      return new byte[0];
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      byte[] serialized = metadata.serialize();
      out.writeInt(serialized.length);
      out.write(serialized);
    }

    private void readObject(ObjectInputStream in
    ) throws IOException, ClassNotFoundException {
      byte[] buf = new byte[in.readInt()];
      in.readFully(buf);
      metadata = new SecureStoreMetadata(buf);
    }
  }
}
