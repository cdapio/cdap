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

package co.cask.cdap.securestore.gcp.cloudkms;

import co.cask.cdap.securestore.spi.SecretManager;
import co.cask.cdap.securestore.spi.SecretManagerContext;
import co.cask.cdap.securestore.spi.SecretNotFoundException;
import co.cask.cdap.securestore.spi.SecretStore;
import co.cask.cdap.securestore.spi.secret.Secret;
import co.cask.cdap.securestore.spi.secret.SecretMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link SecretManager} which manages sensitive data using Google Cloud KMS.
 */
public class CloudSecretManager implements SecretManager {
  private static final Logger LOG = LoggerFactory.getLogger(CloudSecretManager.class);
  private static final String CLOUD_KMS_NAME = "gcp-cloudkms";
  private static final String CRYPTO_KEY_PREFIX = "cdap_key_";
  private final SecretInfoCodec encoderDecoder;
  private SecretStore store;
  private CloudKMSClient client;

  public CloudSecretManager() {
    this.encoderDecoder = new SecretInfoCodec();
  }

  @Override
  public String getName() {
    return CLOUD_KMS_NAME;
  }

  @Override
  public void initialize(SecretManagerContext context) throws IOException {
    this.store = context.getSecretStore();
    this.client = new CloudKMSClient(context.getProperties());
    this.client.createKeyRingIfNotExists();
  }

  @Override
  public void store(String namespace, Secret secret) throws IOException {
    // creates new crypto key for every namespace
    client.createCryptoKeyIfNotExists(CRYPTO_KEY_PREFIX + namespace);
    byte[] encryptedData = client.encrypt(CRYPTO_KEY_PREFIX + namespace, secret.getData());
    SecretMetadata metadata = secret.getMetadata();
    SecretInfo secretInfo = new SecretInfo(metadata.getName(), metadata.getDescription(), encryptedData,
                                           metadata.getCreationTimeMs(), metadata.getProperties());
    store.store(namespace, metadata.getName(), encoderDecoder, secretInfo);
  }

  @Override
  public Secret get(String namespace, String name) throws SecretNotFoundException, IOException {
    SecretInfo secretInfo = store.get(namespace, name, encoderDecoder);
    byte[] decrypted = client.decrypt(CRYPTO_KEY_PREFIX + namespace, secretInfo.getSecretData());
    return new Secret(decrypted, new SecretMetadata(secretInfo.getName(), secretInfo.getDescription(),
                                                    secretInfo.getCreationTimeMs(), secretInfo.getProperties()));
  }

  @Override
  public Collection<SecretMetadata> list(String namespace) throws IOException {
    Collection<SecretInfo> secretInfos = store.list(namespace, encoderDecoder);
    List<SecretMetadata> metadataList = new ArrayList<>();
    for (SecretInfo secretInfo : secretInfos) {
      metadataList.add(new SecretMetadata(secretInfo.getName(), secretInfo.getDescription(),
                                          secretInfo.getCreationTimeMs(), secretInfo.getProperties()));
    }
    return metadataList;
  }

  @Override
  public void delete(String namespace, String name) throws SecretNotFoundException, IOException {
    // Cloud KMS does not provide capability to delete crypto keys.
    // Reference: https://cloud.google.com/kms/docs/faq#cannnot_delete
    store.delete(namespace, name);
  }

  @Override
  public void destroy(SecretManagerContext context) {
    try {
      client.close();
    } catch (IOException e) {
      LOG.warn("Error while closing cloud kms client.", e);
    }
  }
}
