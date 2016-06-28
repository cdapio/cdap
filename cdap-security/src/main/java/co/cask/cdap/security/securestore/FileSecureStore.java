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
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.Charset;
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

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

/**
 * File based implementation of secure store. Uses Java JCEKS based keystore.
 *
 * When the client calls a put, the key is put in the keystore. The system then flushes
 * the keystore to the file system.
 * During the flush, the current file is first written to temporary file (_NEW).
 * If that is successful then the temporary file is renamed atomically to the secure store file.
 * If anything fails during this process then the keystore reverts to the last successfully written file.
 * The keystore is flushed to the filesystem after every put and delete.
 */
class FileSecureStore implements SecureStore, SecureStoreManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileSecureStore.class);

  private static final String SCHEME_NAME = "jceks";
  private static final Gson GSON = new Gson();

  private final char[] password;
  private final Path path;
  private final Lock readLock;
  private final Lock writeLock;
  private final KeyStore keyStore;

  FileSecureStore(CConfiguration cConf) throws IOException {
    // Get the path to the keystore file
    String pathString = cConf.get(Constants.Security.Store.FILE_PATH);
    Path dir = Paths.get(pathString);
    path = dir.resolve(cConf.get(Constants.Security.Store.FILE_NAME));

    // Get the keystore password
    password = cConf.get(Constants.Security.Store.FILE_PASSWORD).toCharArray();

    keyStore = locateKeystore(path, password);
    ReadWriteLock lock = new ReentrantReadWriteLock(true);
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  /**
   * Stores an element in the secure store.
   * @param name Name of the element to store
   * @param data The data that needs to be securely stored
   * @param properties Metadata associated with the data
   * @throws IOException
   */
  @Override
  public void put(String name, byte[] data, Map<String, String> properties) throws IOException {
    SecureStoreMetadata meta = SecureStoreMetadata.of(name, properties);
    SecureStoreData secureStoreData = new SecureStoreData(meta, data);
    writeLock.lock();
    try {
      keyStore.setKeyEntry(name, new KeyStoreEntry(secureStoreData, meta), password, null);
      // Attempt to persist the store.
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
    writeLock.lock();
    try {
      if (keyStore.containsAlias(name)) {
        keyStore.deleteEntry(name);
      }
      // Attempt to persist the store.
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
      Enumeration<String> aliases = keyStore.aliases();
      while (aliases.hasMoreElements()) {
        name = aliases.nextElement();
        list.add(getSecureStoreMetadata(name));
      }
    } catch (KeyStoreException e) {
      throw new IOException("Failed to get the list of elements from the secure store.", e);
    } finally {
      readLock.unlock();
    }
    return list;
  }

  /**
   * @param name Name of the data element.
   * @return An object representing the securely stored data associated with the name.
   */
  @Override
  public SecureStoreData get(String name) throws IOException {
    readLock.lock();
    try {
      if (!keyStore.containsAlias(name)) {
        throw new IOException(name + " not found in the secure store.");
      }
      Key key = keyStore.getKey(name, password);
      return ((KeyStoreEntry) key).data;
    } catch (NoSuchAlgorithmException | UnrecoverableKeyException | KeyStoreException e) {
      throw new IOException("Unable to retrieve the key " + name, e);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns the metadata for the element identified by the given name.
   * @param name Name of the element
   * @return An object representing the metadata associated with the element
   * @throws IOException
   */
  private SecureStoreMetadata getSecureStoreMetadata(String name) throws IOException {
    readLock.lock();
    try {
      if (!keyStore.containsAlias(name)) {
        throw new IOException("Metadata for " + name + " not found in the secure store.");
      }
      Key key = keyStore.getKey(name, password);
      return ((KeyStoreEntry) key).metadata;
    } catch (NoSuchAlgorithmException | UnrecoverableKeyException | KeyStoreException e) {
      throw new IOException("Unable to retrieve the metadata for " + name, e);
    } finally {
      readLock.unlock();
    }
  }

  private static Path constructNewPath(Path path) {
    return Paths.get(path.toString() + "_NEW");
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

  /**
   * Initialize the keyStore.
   *
   * @throws IOException If there is a problem reading or creating the keystore.
   */
  private static KeyStore locateKeystore(Path path, final char[] password) throws IOException {
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
        tryLoadFromPath(ks, path, newPath, password);
      } else {
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
   * @param backupPath Backup path (_NEW)
   * @throws IOException
   */
  private static void tryLoadFromPath(KeyStore keyStore, Path path, Path backupPath, char[] password)
    throws IOException {
    try {
      loadFromPath(keyStore, path, password);
      // Successfully loaded the keystore. No need to keep the backup file.
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
   * Persist the keystore on the file system.
   *
   * During the flush the steps are
   *  1. Mark existing _NEW as orphaned, it will exist only if something had failed in the last run.
   *  2. Try to write the current keystore in a _NEW file.
   *  3. If something fails then revert the key store to the old state and throw IOException.
   *  4. If everything is OK then rename the _NEW to the main file.
   *
   * @throws IOException
   */
  private void flush() throws IOException {
    Path newPath = constructNewPath(path);
    writeLock.lock();
    try {
      // Might exist if a backup has been restored etc.
      if (Files.exists(newPath)) {
        Files.move(newPath, Paths.get(newPath.toString() + "_ORPHANED_" + System.currentTimeMillis()));
      }

      // Flush the keystore, write the _NEW file first
      writeToNew(newPath);
      // Do Atomic rename _NEW to CURRENT
      Files.move(newPath, path, ATOMIC_MOVE);
    } catch (IOException ioe) {
      resetKeyStoreState(path);
      LOG.error("Failed to persist the key store.", ioe);
      throw ioe;
    } finally {
      writeLock.unlock();
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

  /**
   * An adapter between a KeyStore Key and our SecureStoreMetadata. This is used to store
   * the metadata in a KeyStore even though isn't really a key.
   */
  private static class KeyStoreEntry implements Key, Serializable {
    private static final long serialVersionUID = 3405839418917868651L;
    private static final String METADATA_FORMAT = "KeyStoreEntry";
    // Java Keystore needs an algorithm name to store a key, it is not used for any checks, only stored,
    // since we are not handling the encryption we don't care about this.
    private static final String ALGORITHM_PROXY = "none";

    private SecureStoreData data;
    private SecureStoreMetadata metadata;

    private KeyStoreEntry(SecureStoreData data, SecureStoreMetadata meta) {
      this.data = data;
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

    /**
     * Serialize the metadata to a byte array.
     *
     * @return the serialized bytes
     * @throws IOException
     */
    private static byte[] serializeMetadata(SecureStoreMetadata metadata) throws IOException {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      OutputStreamWriter out = new OutputStreamWriter(buffer, Charset.forName("UTF-8"));
      GSON.toJson(metadata, out);
      out.flush();
      return buffer.toByteArray();
    }

    /**
     * Deserialize a new metadata object from a byte array.
     *
     * @param buf the serialized metadata
     * @throws IOException
     */
    private static SecureStoreMetadata deserializeMetadata(byte[] buf) throws IOException {
      try (JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(buf),
                                                                    Charset.forName("UTF-8")))) {
        return GSON.fromJson(reader, SecureStoreMetadata.class);
      }
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      byte[] serializedMetadata = serializeMetadata(metadata);
      byte[] binaryData = data.get();

      out.writeInt(serializedMetadata.length);
      out.write(serializedMetadata);
      out.writeInt(binaryData.length);
      out.write(binaryData);
    }

    private void readObject(ObjectInputStream in
    ) throws IOException {
      byte[] buf = new byte[in.readInt()];
      in.readFully(buf);
      byte[] dataBuf = new byte[in.readInt()];
      in.readFully(dataBuf);
      metadata = deserializeMetadata(buf);
      data = new SecureStoreData(metadata, dataBuf);
    }
  }
}
