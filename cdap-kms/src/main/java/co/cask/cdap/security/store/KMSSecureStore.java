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
import co.cask.cdap.common.security.DelegationTokensUpdater;
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
import java.util.List;
import java.util.Map;

/**
 * Secure Store implementation backed by Hadoop KMS. This class is loaded using reflection if
 * the provider is set to kms and Hadoop version is 2.6.0 or higher.
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

  /**
   * Sets up the key provider.
   * @param conf Hadoop configuration. core-site.xml contains the key provider URI.
   * @throws URISyntaxException If the key provider path is not a valid URI.
   * @throws IOException If the authority or the port could not be read from the provider URI.
   */
  @Inject
  KMSSecureStore(Configuration conf) throws IOException, URISyntaxException {
    this.conf = conf;
    try {
      URI providerUri = new URI(conf.get(KeyProviderFactory.KEY_PROVIDER_PATH));
      provider = KMSClientProvider.Factory.get(providerUri, conf);
    } catch (URISyntaxException e) {
      throw new URISyntaxException("Secure store could not be loaded. The value for hadoop.security.key.provider.path" +
                                     "in core-site.xml is not a valid URI.", e.getReason());
    } catch (IOException e) {
      throw new IOException("Secure store could not be loaded. KMS KeyProvider failed to initialize", e);
    }
    LOG.debug("Secure Store initialized successfully.");
  }

  /**
   * Stores an element in the secure store. The key is stored as namespace:name in the backing store,
   * assuming ":" to be the separator.
   * @param namespace The namespace this key belongs to.
   * @param name Name of the element to store.
   * @param data The data that needs to be securely stored.
   * @param description User provided description of the entry.
   * @param properties Metadata associated with the data
   * @throws IOException If it failed to store the key in the store.
   */
  @Override
  public void putSecureData(String namespace, String name, byte[] data, String description,
                            Map<String, String> properties) throws IOException {
    KeyProvider.Options options = new KeyProvider.Options(conf);
    options.setDescription(description);
    options.setAttributes(properties);
    options.setBitLength(data.length * Byte.SIZE);
    String keyName = getKeyName(namespace, name);
    try {
      provider.createKey(keyName, data, options);
    } catch (IOException e) {
      throw new IOException("Failed to store the key. " + name + " under namespace " + namespace, e);
    }
  }

  /**
   * Deletes the element with the given name.
   * @param namespace The namespace this key belongs to.
   * @param name Name of the element to be deleted.
   */
  @Override
  public void deleteSecureData(String namespace, String name) throws IOException {
    try {
      provider.deleteKey(getKeyName(namespace, name));
    } catch (IOException e) {
      throw new IOException("Failed to delete the key. " + name + " under namespace " + namespace, e);
    }
  }

  /**
   * List of all the entries in the secure store.
   * @return A list of {@link SecureStoreMetadata} objects representing the data stored in the store.
   * @param namespace The namespace this key belongs to.
   */
  @Override
  public List<SecureStoreMetadata> listSecureData(String namespace) throws IOException {
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
   * @param namespace The namespace this key belongs to.
   * @param name Name of the data element.
   * @return An object representing the securely stored data associated with the name.
   */
  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws IOException {
    String keyName = getKeyName(namespace, name);
    KeyProvider.Metadata metadata = provider.getMetadata(keyName);
    SecureStoreMetadata meta = SecureStoreMetadata.of(name, metadata.getDescription(), metadata.getAttributes());
    KeyProvider.KeyVersion keyVersion = provider.getCurrentKey(keyName);
    return new SecureStoreData(meta, keyVersion.getMaterial());
  }

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

  private static String getKeyName(final String namespace, final String name) {
    return namespace + NAME_SEPARATOR + name;
  }
}
