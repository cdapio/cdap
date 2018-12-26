/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.securestore.spi;

import co.cask.cdap.securestore.spi.secret.Secret;
import co.cask.cdap.securestore.spi.secret.SecretMetadata;

import java.util.Map;

/**
 * Secrets manager implementation which uses Google Cloud KMS to encrypt and decrypt secrets.
 */
public class CloudSecretsManager implements SecretsManager {
  private SecretsMetadataStore<Secret> store;
  private CloudKMSClient client;

  @Override
  public String getSecretsManagerName() {
    return "cloudkms";
  }

  @Override
  public void initialize(SecretsManagerContext context) throws Exception {
    this.client = new CloudKMSClient(new CloudKMSConf());
    // TODO CDAP-14699 Get metadata store from secrets manager context
    this.store = new MockSecretsMetadataStore();
  }

  @Override
  public void storeSecret(String namespace, String name, byte[] data, String description,
                          Map<String, String> properties) throws Exception {
    client.createCryptoKey(namespace);
    byte[] encryptedData = client.encrypt(namespace, data);
    store.store(namespace, name, new Secret(encryptedData, new SecretMetadata(name, description,
                                                                              System.currentTimeMillis(), properties)));
  }

  @Override
  public Secret getSecret(String namespace, String name) throws Exception {
    Secret secret = store.get(namespace, name);
    byte[] decrypted = client.decrypt(namespace, secret.getData());
    return new Secret(decrypted, secret.getMetadata());
  }

  @Override
  public Map<String, String> listSecrets(String namespace) throws Exception {
    return store.list(namespace);
  }

  @Override
  public void deleteSecret(String namespace, String name) throws Exception {
    // Cloud KMS does not provide capability to delete crypto keys. So just delete data from secrets metadata store.
    // https://cloud.google.com/kms/docs/faq#cannnot_delete
    store.delete(namespace, name);
  }
}
