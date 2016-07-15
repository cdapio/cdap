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

  private final KeyProvider provider;
  private final Configuration conf;

  @Inject
  public KMSSecureStore(Configuration conf) throws IOException, URISyntaxException {
    this.conf = conf;
    URI providerUri = new URI(conf.get(KeyProviderFactory.KEY_PROVIDER_PATH));
    provider = KMSClientProvider.Factory.get(providerUri, conf);
  }

  @Override
  public void put(String namespace, String name, byte[] data, String description, Map<String, String> properties)
    throws IOException {
    KeyProvider.Options options = new KeyProvider.Options(conf);
    options.setDescription(description);
    options.setAttributes(properties);
    options.setBitLength(data.length * Byte.SIZE);
    provider.createKey(SecureStoreUtils.getKeyName(namespace, name), data, options);
  }

  @Override
  public void delete(String namespace, String name) throws IOException {
    provider.deleteKey(SecureStoreUtils.getKeyName(namespace, name));
  }

  @Override
  public List<SecureStoreMetadata> list(String namespace) throws IOException {
    String prefix = namespace + SecureStoreUtils.NAME_SEPARATOR;
    List<String> keysInNamespace = new ArrayList<>();
    for (String key : provider.getKeys()) {
      if (key.startsWith(prefix)) {
        keysInNamespace.add(key);
      }
    }
    KeyProvider.Metadata[] metadatas = provider.getKeysMetadata(keysInNamespace.toArray(new String[0]));
    List<SecureStoreMetadata> list = new ArrayList<>();
    for (int i = 0; i < metadatas.length; i++) {
      KeyProvider.Metadata metadata = metadatas[i];
      SecureStoreMetadata meta = SecureStoreMetadata.of(keysInNamespace.get(i).substring(prefix.length()),
                                                        metadata.getDescription(),
                                                        metadata.getAttributes());
      list.add(meta);
    }
    return list;
  }

  @Override
  public SecureStoreData get(String namespace, String name) throws IOException {
    String keyName = SecureStoreUtils.getKeyName(namespace, name);
    KeyProvider.Metadata metadata = provider.getMetadata(keyName);
    SecureStoreMetadata meta = SecureStoreMetadata.of(name, metadata.getDescription(), metadata.getAttributes());
    KeyProvider.KeyVersion keyVersion = provider.getCurrentKey(keyName);
    return new SecureStoreData(meta, keyVersion.getMaterial());
  }

  public KeyProvider getProvider() {
    return provider;
  }
}
