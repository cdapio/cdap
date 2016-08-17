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

package co.cask.cdap.security.store;

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.security.DelegationTokensUpdater;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.SecureKeyId;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.security.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;

/**
 * Secure Store implementation backed by Hadoop KMS. This assumes that KMS is setup and running.
 * The URI for KMS endpoint is retrieved from Hadoop core-site.xml. The property that has the URI
 * is "hadoop.security.key.provider.path". The ACL for KMS is in kms-acls.xml located by default under
 * "/etc/hadoop-kms/conf/".
 * This class is loaded using reflection if the provider is set to kms and Hadoop version is 2.6.0 or higher.
 * The class is public to allow javadoc to build.
 */
// TODO: Find a better way to handle javadoc so this class does not need to be public.
@SuppressWarnings("unused")
public class KMSSecureStore implements SecureStore, SecureStoreManager, DelegationTokensUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(KMSSecureStore.class);
  /** Separator between the namespace name and the key name */
  private static final String NAME_SEPARATOR = ":";
  /**
   * Hadoop KeyProvider interface. This is used to interact with KMS.
   */
  private final KeyProvider provider;
  private final Configuration conf;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  /**
   * Sets up the key provider. It reads the KMS URI from Hadoop conf to initialize the provider.
   * @param conf Hadoop configuration. core-site.xml contains the key provider URI.
   * @param namespaceQueryAdmin For querying namespace.
   * @throws IllegalArgumentException If the key provider URI is not set.
   * @throws URISyntaxException If the key provider path is not a valid URI.
   * @throws IOException If the authority or the port could not be read from the provider URI.
   */
  @Inject
  KMSSecureStore(Configuration conf, NamespaceQueryAdmin namespaceQueryAdmin) throws IOException, URISyntaxException {
    this.conf = conf;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    try {
      String keyProviderPath = conf.get(KeyProviderFactory.KEY_PROVIDER_PATH);
      if (Strings.isNullOrEmpty(keyProviderPath)) {
        throw new IllegalArgumentException("Could not find the key provider URI. Please make sure that " +
                                             "hadoop.security.key.provider.path is set to the KMS URI in your " +
                                             "core-site.xml.");
      }
      URI providerUri = new URI(keyProviderPath);
      provider = KMSClientProvider.Factory.get(providerUri, conf);
    } catch (URISyntaxException e) {
      throw new URISyntaxException("Secure store could not be loaded. The value for hadoop.security.key.provider.path" +
                                     "in core-site.xml is not a valid URI.", e.getReason());
    } catch (IOException e) {
      throw new IOException("Secure store could not be loaded. KMS KeyProvider failed to initialize", e);
    }
    LOG.debug("KMS backed secure store initialized successfully.");
  }

  /**
   * Stores an element in the secure store. The key is stored as namespace:name in the backing store,
   * assuming ":" is the name separator.
   * @param namespace The namespace this key belongs to.
   * @param name Name of the element to store.
   * @param data The data that needs to be securely stored.
   * @param description User provided description of the entry.
   * @param properties Metadata associated with the data
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws IOException If it failed to store the key in the store.
   */
  // Unfortunately KeyProvider does not specify
  // the underlying cause except in the message, so we can not throw a more specific exception.
  @Override
  public void putSecureData(String namespace, String name, String data, String description,
                            Map<String, String> properties) throws Exception {
    checkNamespaceExists(namespace);
    KeyProvider.Options options = new KeyProvider.Options(conf);
    options.setDescription(description);
    options.setAttributes(properties);
    byte[] buff = data.getBytes(Charsets.UTF_8);
    options.setBitLength(buff.length * Byte.SIZE);
    String keyName = getKeyName(namespace, name);
    try {
      provider.createKey(keyName, buff, options);
    } catch (IOException e) {
      throw new IOException("Failed to store the key " + name + " under namespace " + namespace, e);
    }
  }

  /**
   * Deletes the element with the given name. An IOException is thrown if the key does not exist. We can not check the
   * existence of the key and then delete it atomically. So, there is no easy way to disambiguate between the key
   * not existing or a failure to delete because of some other reason apart from the message in the exception.
   * @param namespace The namespace this key belongs to.
   * @param name Name of the element to be deleted.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws IOException If it failed to delete the key in the store. Unfortunately KeyProvider does not specify
   * the underlying cause except in the message, so we can not throw a more specific exception.
   */
  @Override
  public void deleteSecureData(String namespace, String name) throws Exception {
    checkNamespaceExists(namespace);
    try {
      provider.deleteKey(getKeyName(namespace, name));
    } catch (IOException e) {
      throw new IOException("Failed to delete the key " + name + " under namespace " + namespace, e);
    }
  }

  /**
   * List of all the entries in the secure store. No filtering or authentication is done here.
   * This method makes two calls to the KMS provider, one to get the list of keys and then another call to
   * get the metadata for all the keys in the requested namespace.
   * @return A list of {@link SecureStoreMetadata} objects representing the data stored in the store.
   * @param namespace The namespace this key belongs to.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws ConcurrentModificationException If a key was deleted between the time we got the list of keys and when
   * we got their metadata.
   * @throws IOException If there was a problem getting the list from the underlying key provider.
   */

  // Unfortunately KeyProvider does not specify the underlying cause except in the message, so we can not throw a
  // more specific exception.
  @Override
  public List<SecureStoreMetadata> listSecureData(String namespace) throws Exception {
    checkNamespaceExists(namespace);
    String prefix = namespace + NAME_SEPARATOR;
    List<String> keysInNamespace = new ArrayList<>();
    KeyProvider.Metadata[] metadatas;
    try {
      for (String key : provider.getKeys()) {
        if (key.startsWith(prefix)) {
          keysInNamespace.add(key);
        }
      }
      metadatas = provider.getKeysMetadata(keysInNamespace.toArray(new String[keysInNamespace.size()]));
    } catch (IOException e) {
      throw new IOException("Failed to get the list of elements from the secure store.", e);
    }
    // If a key was deleted between the time we get the list of keys and their metadatas then throw an exception
    if (metadatas.length != keysInNamespace.size()) {
      throw new ConcurrentModificationException("A key was deleted while listing was in progress. Please try again.");
    }
    List<SecureStoreMetadata> secureStoreMetadatas = new ArrayList<>(metadatas.length);
    for (int i = 0; i < metadatas.length; i++) {
      KeyProvider.Metadata metadata = metadatas[i];
      SecureStoreMetadata meta = SecureStoreMetadata.of(keysInNamespace.get(i).substring(prefix.length()),
                                                        metadata.getDescription(),
                                                        metadata.getAttributes());
      secureStoreMetadatas.add(meta);
    }
    return secureStoreMetadatas;
  }

  /**
   * Returns the data stored in the secure store. Makes two calls to the provider, one to get the metadata and another
   * to get the data.
   * @param namespace The namespace this key belongs to.
   * @param name Name of the key.
   * @return An object representing the securely stored data associated with the name.
   * @throws NamespaceNotFoundException If the specified namespace does not exist.
   * @throws IOException If there was a problem getting the key or the metadata from the underlying key provider.
   */
  // Unfortunately KeyProvider does not specify the underlying cause except in the message, so we can not throw a
  // more specific exception.
  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws Exception {
    checkNamespaceExists(namespace);
    String keyName = getKeyName(namespace, name);
    KeyProvider.Metadata metadata = provider.getMetadata(keyName);
    // Provider returns null if the key is not found.
    if (metadata == null) {
      throw new NotFoundException(new SecureKeyId(namespace, name));
    }
    SecureStoreMetadata meta = SecureStoreMetadata.of(name, metadata.getDescription(), metadata.getAttributes());
    KeyProvider.KeyVersion keyVersion = provider.getCurrentKey(keyName);
    return new SecureStoreData(meta, keyVersion.getMaterial());
  }

  /**
   * Uses the KeyProviderDelegationTokenExtension to get the delegation token for KMS.
   * @param renewer User used to renew the delegation tokens
   * @param credentials Credentials in which to add new delegation tokens
   * @return credentials with KMS delegation token added if it was successfully retrieved.
   */
  @Override
  public Credentials addDelegationTokens(String renewer, Credentials credentials) {
    KeyProviderDelegationTokenExtension tokenExtension =
      KeyProviderDelegationTokenExtension.createKeyProviderDelegationTokenExtension(provider);
    try {
      tokenExtension.addDelegationTokens(renewer, credentials);
    } catch (IOException e) {
      LOG.debug("KMS delegation token not updated.");
    }
    return credentials;
  }

  private void checkNamespaceExists(String namespace) throws Exception {
    Id.Namespace namespaceId = new Id.Namespace(namespace);
    if (!namespaceQueryAdmin.exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }
  }

  private static String getKeyName(final String namespace, final String name) {
    return namespace + NAME_SEPARATOR + name;
  }
}
