/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.SecureKeyId;
import io.cdap.cdap.security.store.file.FileSecureStoreCodec;
import io.cdap.cdap.security.store.file.KeyInfo;
import io.cdap.cdap.security.store.file.SecureStoreDataCodecV2;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import javax.annotation.Nullable;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File based implementation of secure store. Uses Java PKCS12 based keystore.
 *
 * When the client calls a put, the key is put in the keystore. The system then flushes the keystore
 * to the file system. During the flush, the current file is first written to temporary file (_NEW).
 * If that is successful then the temporary file is renamed atomically to the secure store file. If
 * anything fails during this process then the keystore reverts to the last successfully written
 * file. The keystore is flushed to the filesystem after every put and delete.
 *
 * This class is marked as Singleton because it won't work if this class is not a Singleton. Setting
 * in(Scopes.Singleton) in the bindings doesn't work because we are binding this class to two
 * interfaces and we need the instance to be shared between them.
 */
@Singleton
public class FileSecureStoreService extends AbstractIdleService implements SecureStoreService {

  private static final Logger LOG = LoggerFactory.getLogger(FileSecureStoreService.class);
  static final String SECRET_KEY_FACTORY_ALGORITHM = "PBE";

  /**
   * The current codec for file-based secure store in use. This codec class must be updated upon
   * backwards-incompatible changes to the serialization methods.
   */
  public static final Class<? extends FileSecureStoreCodec> CURRENT_CODEC = SecureStoreDataCodecV2.class;

  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final char[] password;
  private final Path path;
  private final Lock readLock;
  private final Lock writeLock;
  private final KeyStore keyStore;
  private final FileSecureStoreCodec fileSecureStoreCodec;

  @Inject
  public FileSecureStoreService(CConfiguration cConf, SConfiguration sConf,
      NamespaceQueryAdmin namespaceQueryAdmin,
      FileSecureStoreCodec fileSecureStoreCodec) throws IOException {
    // Get the path to the keystore file
    String pathString = cConf.get(Constants.Security.Store.FILE_PATH);
    Path dir = Paths.get(pathString);
    path = dir.resolve(cConf.get(Constants.Security.Store.FILE_NAME));
    // Get the keystore password
    password = sConf.get(Constants.Security.Store.FILE_PASSWORD).toCharArray();
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.fileSecureStoreCodec = fileSecureStoreCodec;

    keyStore = locateKeystore(path, password);
    ReadWriteLock lock = new ReentrantReadWriteLock(true);
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @VisibleForTesting
  FileSecureStoreService(NamespaceQueryAdmin namespaceQueryAdmin, char[] password, Path path,
      KeyStore keyStore,
      FileSecureStoreCodec fileSecureStoreCodec) {
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.password = password;
    this.path = path;
    this.keyStore = keyStore;
    this.fileSecureStoreCodec = fileSecureStoreCodec;
    ReadWriteLock lock = new ReentrantReadWriteLock(true);
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  /**
   * Stores an element in the secure store. If the element already exists, it will get overwritten.
   *
   * @param namespace The namespace this key belongs to.
   * @param name Name of the element to store.
   * @param data The data that needs to be securely stored.
   * @param description User provided description of the entry.
   * @param properties Metadata associated with the data.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws IOException If there was a problem storing the key to the in memory keystore or if
   *     there was problem persisting the keystore.
   */
  @Override
  public void put(String namespace, String name, String data, @Nullable String description,
      Map<String, String> properties) throws Exception {
    checkNamespaceExists(namespace);
    String keyName = fileSecureStoreCodec.getKeyAliasFromInfo(new KeyInfo(namespace, name));
    SecureStoreMetadata meta = new SecureStoreMetadata(name, description,
        System.currentTimeMillis(), properties);
    byte[] dataBytes = data.getBytes(Charsets.UTF_8);
    SecureStoreData secureStoreData = new SecureStoreData(meta, dataBytes);
    writeLock.lock();
    try {
      SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(
          SECRET_KEY_FACTORY_ALGORITHM);
      // Convert byte[] directly to char[] and avoid using String due
      // to it being stored in memory until garbage collected.
      PBEKeySpec pbeKeySpec = new PBEKeySpec(StandardCharsets.UTF_8
          .decode(ByteBuffer.wrap(fileSecureStoreCodec.encode(secureStoreData)))
          .array());
      keyStore.setKeyEntry(keyName, secretKeyFactory.generateSecret(pbeKeySpec), password, null);
      // Attempt to persist the store.
      flush();
      LOG.debug(String.format("Successfully stored %s in namespace %s", name, namespace));
    } catch (KeyStoreException e) {
      // We failed to store the key in the key store. Throw an IOException.
      throw new IOException("Failed to store the key. ", e);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Deletes the element with the given name. Flushes the keystore after deleting the key from the
   * in memory keystore. If the flush fails, we attempt to insert to key back to the in memory store
   * and notify the user that delete failed. If the insertion in the key store fails after a flush
   * failure then there would be a discrepancy between the in memory store and the file on the disk.
   * This will be remedied the next time a flush happens. If another flush does not happen and the
   * system is restarted, the only time that file is read, then we will have an extra key in the
   * keystore.
   *
   * @param namespace The namespace this key belongs to.
   * @param name Name of the element to be deleted.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws NotFoundException If the key to be deleted is not found.
   * @throws IOException If their was a problem during deleting the key from the in memory store
   *     or if there was a problem persisting the keystore after deleting the element.
   */
  @Override
  public void delete(String namespace, String name) throws Exception {
    checkNamespaceExists(namespace);
    String keyName = fileSecureStoreCodec.getKeyAliasFromInfo(new KeyInfo(namespace, name));
    Key key = null;
    writeLock.lock();
    try {
      if (!keyStore.containsAlias(keyName)) {
        throw new NotFoundException(new SecureKeyId(namespace, name));
      }
      key = deleteFromStore(keyName, password);
      flush();
      LOG.debug(String.format("Successfully deleted key %s from namespace %s", name, namespace));
    } catch (UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException e) {
      throw new IOException("Failed to delete the key. ", e);
    } catch (IOException ioe) {
      try {
        keyStore.setKeyEntry(keyName, key, password, null);
      } catch (KeyStoreException e) {
        ioe.addSuppressed(e);
      }
      throw ioe;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * List of all the entries in the secure store belonging to the specified namespace. No filtering
   * or authentication is done here.
   *
   * @param namespace The namespace this key belongs to.
   * @return A list of {@link SecureStoreMetadata} objects representing the data stored in the
   *     store.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws IOException If there was a problem reading from the keystore.
   */
  @Override
  public List<SecureStoreMetadata> list(String namespace) throws Exception {
    checkNamespaceExists(namespace);
    readLock.lock();
    try {
      Enumeration<String> aliases = keyStore.aliases();
      List<SecureStoreMetadata> metadataList = new ArrayList<>();
      String prefix = fileSecureStoreCodec.getAliasSearchPrefix(namespace);
      while (aliases.hasMoreElements()) {
        String alias = aliases.nextElement();
        // Filter out elements not in this namespace.
        if (alias.startsWith(prefix)) {
          metadataList.add(getSecureStoreMetadata(alias));
        }
      }
      return metadataList;
    } catch (KeyStoreException e) {
      throw new IOException("Failed to get the list of elements from the secure store.", e);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Returns the data stored in the secure store.
   *
   * @param namespace The namespace this key belongs to.
   * @param name Name of the data element.
   * @return An object representing the securely stored data associated with the name.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws NotFoundException If the key is not found in the store.
   * @throws IOException If there was a problem reading from the store.
   */
  @Override
  public SecureStoreData get(String namespace, String name) throws Exception {
    checkNamespaceExists(namespace);
    String keyName = fileSecureStoreCodec.getKeyAliasFromInfo(new KeyInfo(namespace, name));
    readLock.lock();
    try {
      if (!keyStore.containsAlias(keyName)) {
        throw new NotFoundException(name + " not found in the secure store.");
      }
      Key key = keyStore.getKey(keyName, password);
      return fileSecureStoreCodec.decode(key.getEncoded());
    } catch (NoSuchAlgorithmException | UnrecoverableKeyException | KeyStoreException e) {
      throw new IOException("Unable to retrieve the key " + name, e);
    } finally {
      readLock.unlock();
    }
  }

  private void checkNamespaceExists(String namespace) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    if (!NamespaceId.SYSTEM.equals(namespaceId) && !namespaceQueryAdmin.exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }
  }

  private Key deleteFromStore(String name, char[] password) throws KeyStoreException,
      UnrecoverableKeyException, NoSuchAlgorithmException {
    writeLock.lock();
    try {
      // TODO CDAP-18903: Delete sensitive data after usage for SecureStoreData.
      Key key = keyStore.getKey(name, password);
      keyStore.deleteEntry(name);
      return key;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Returns the metadata for the element identified by the given name. The name must be of the
   * format namespace + NAME_SEPARATOR + key name.
   *
   * @param keyName Name of the element
   * @return An object representing the metadata associated with the element
   * @throws NotFoundException If the key was not found in the store.
   * @throws IOException If there was a problem in getting the key from the store
   */
  private SecureStoreMetadata getSecureStoreMetadata(String keyName) throws Exception {
    KeyInfo keyInfo = fileSecureStoreCodec.getKeyInfoFromAlias(keyName);
    String namespace = keyInfo.getNamespace();
    String name = keyInfo.getName();
    readLock.lock();
    try {
      if (!keyStore.containsAlias(keyName)) {
        throw new NotFoundException(new SecureKeyId(namespace, name));
      }
      Key key = keyStore.getKey(keyName, password);
      // TODO CDAP-18903: Delete sensitive data after usage for SecureStoreData.
      return fileSecureStoreCodec.decode(key.getEncoded()).getMetadata();
    } catch (NoSuchAlgorithmException | UnrecoverableKeyException | KeyStoreException e) {
      throw new IOException(
          "Unable to retrieve the metadata for " + name + " in namespace " + namespace, e);
    } finally {
      readLock.unlock();
    }
  }

  private static Path constructNewPath(Path path) {
    return path.resolveSibling(path.getFileName() + "_NEW");
  }

  /**
   * Initialize the keyStore.
   *
   * @throws IOException If there is a problem reading or creating the keystore.
   */
  private KeyStore locateKeystore(Path path, final char[] password) throws IOException {
    Path newPath = constructNewPath(path);
    KeyStore ks;
    try {
      ks = KeyStore.getInstance(fileSecureStoreCodec.getKeyStoreScheme());
      Files.deleteIfExists(newPath);
      if (Files.exists(path)) {
        FileSecureStoreImporter importer = new FileSecureStoreImporter(fileSecureStoreCodec);
        importer.importFromPath(ks, path, password);
      } else {
        Path parent = path.getParent();
        if (!Files.exists(parent)) {
          Files.createDirectories(parent);
        }
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
   * Persist the keystore on the file system.
   *
   * During the flush the steps are 1. Delete the _NEW file if it exists, it will exist only if
   * something had failed in the last run. 2. Try to write the current keystore in a _NEW file. 3.
   * If something fails then revert the key store to the old state and throw IOException. 4. If
   * everything is OK then rename the _NEW to the main file.
   */
  private void flush() throws IOException {
    Path newPath = constructNewPath(path);
    writeLock.lock();
    try {
      // Might exist if a backup has been restored etc.
      Files.deleteIfExists(newPath);

      // Flush the keystore, write the _NEW file first
      writeToKeyStore(newPath);
      // Do Atomic rename _NEW to CURRENT
      Files.move(newPath, path, ATOMIC_MOVE, REPLACE_EXISTING);
    } finally {
      writeLock.unlock();
    }
  }

  private void writeToKeyStore(Path newPath) throws IOException {
    try (OutputStream fos = new DataOutputStream(Files.newOutputStream(newPath))) {
      keyStore.store(fos, password);
    } catch (KeyStoreException e) {
      throw new IOException("The underlying java key store has not been initialized.", e);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(
          "The appropriate data integrity algorithm for the underlying java key store could not "
              + "be found", e);
    } catch (CertificateException e) {
      // Should not happen as we are not storing certificates in the keystore.
      throw new IOException("Failed to store the certificates included in the keystore data.", e);
    }
  }

  @Override
  protected void startUp() throws Exception {
    // no-op
  }

  @Override
  protected void shutDown() throws Exception {
    // no-op
  }
}
