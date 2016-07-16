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
import co.cask.cdap.common.security.SecureStoreUtils;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Secure Store implementation backed by Hadoop KMS
 */
public class KMSSecureStore implements SecureStore, SecureStoreManager {

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
  public KMSSecureStore(Configuration conf) throws IOException, URISyntaxException {
    this.conf = conf;
    URI providerUri = new URI(conf.get(KeyProviderFactory.KEY_PROVIDER_PATH));
    provider = KMSClientProvider.Factory.get(providerUri, conf);
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
  public void put(String namespace, String name, byte[] data, String description, Map<String, String> properties)
    throws IOException {
    KeyProvider.Options options = new KeyProvider.Options(conf);
    options.setDescription(description);
    options.setAttributes(properties);
    options.setBitLength(data.length * Byte.SIZE);
    try {
      provider.createKey(SecureStoreUtils.getKeyName(namespace, name), data, options);
    } catch (IOException e) {
      throw new IOException("Failed to store the key. ", e);
    }
  }

  @Override
  public void delete(String namespace, String name) throws IOException {
    try {
      provider.deleteKey(SecureStoreUtils.getKeyName(namespace, name));
    } catch (IOException e) {
      throw new IOException("Failed to delete the key. ", e);
    }
  }

  @Override
  public List<SecureStoreMetadata> list(String namespace) throws IOException {
    String prefix = namespace + SecureStoreUtils.NAME_SEPARATOR;
    List<String> keysInNamespace = new ArrayList<>();
    KeyProvider.Metadata[] metadatas;
    List<SecureStoreMetadata> secureStoreMetadatas;
    try {
      for (String key : provider.getKeys()) {
        if (key.startsWith(prefix)) {
          keysInNamespace.add(key);
        }
      }
      metadatas = provider.getKeysMetadata(keysInNamespace.toArray(new String[0]));
      secureStoreMetadatas = new ArrayList<>();
    } catch (IOException e) {
      throw new IOException("Failed to get the list of elements from the secure store.", e);
    }
    for (int i = 0; i < metadatas.length; i++) {
      KeyProvider.Metadata metadata = metadatas[i];
      SecureStoreMetadata meta = SecureStoreMetadata.of(keysInNamespace.get(i).substring(prefix.length()),
                                                        metadata.getDescription(),
                                                        metadata.getAttributes());
      secureStoreMetadatas.add(meta);
    }
    return secureStoreMetadatas;
  }

  @Override
  public SecureStoreData get(String namespace, String name) throws IOException {
    String keyName = SecureStoreUtils.getKeyName(namespace, name);
    KeyProvider.Metadata metadata = provider.getMetadata(keyName);
    SecureStoreMetadata meta = SecureStoreMetadata.of(name, metadata.getDescription(), metadata.getAttributes());
    KeyProvider.KeyVersion keyVersion = provider.getCurrentKey(keyName);
    return new SecureStoreData(meta, keyVersion.getMaterial());
  }

  KeyProvider getProvider() {
    return provider;
  }
}
