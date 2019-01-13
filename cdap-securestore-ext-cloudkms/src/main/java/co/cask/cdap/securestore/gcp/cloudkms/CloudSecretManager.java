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
  private SecretStore store;
  private CloudKMSClient client;
  private SecretEncoderDecoder encoderDecoder;

  @Override
  public String getName() {
    return "cloudkms";
  }

  @Override
  public void initialize(SecretManagerContext context) throws IOException {
    this.store = context.getSecretStore();
    this.client = new CloudKMSClient();
    this.client.createKeyRing();
    this.encoderDecoder = new SecretEncoderDecoder();
  }

  @Override
  public void store(String namespace, Secret secret) throws IOException {
    // creates new crypto key for every namespace
    client.createCryptoKey(namespace);
    byte[] encryptedData = client.encrypt(namespace, secret.getData());
    Secret encryptedSecret = new Secret(encryptedData, secret.getMetadata());
    store.store(namespace, secret.getMetadata().getName(), encoderDecoder, encryptedSecret);
  }

  @Override
  public Secret get(String namespace, String name) throws SecretNotFoundException, IOException {
    Secret secret = store.get(namespace, name, encoderDecoder);
    byte[] decrypted = client.decrypt(namespace, secret.getData());
    return new Secret(decrypted, secret.getMetadata());
  }

  @Override
  public Collection<SecretMetadata> list(String namespace) throws IOException {
    Collection<Secret> secrets = store.list(namespace, encoderDecoder);
    List<SecretMetadata> metadata = new ArrayList<>();
    for (Secret secret : secrets) {
      LOG.info("######## Secret Name: {}", secret.getMetadata().getName());
      metadata.add(secret.getMetadata());
    }
    return metadata;
  }

  @Override
  public void delete(String namespace, String name) throws SecretNotFoundException, IOException {
    // Cloud KMS does not provide capability to delete crypto keys.
    // Reference: https://cloud.google.com/kms/docs/faq#cannnot_delete
    store.delete(namespace, name);
  }

  @Override
  public void destroy(SecretManagerContext context) {
    // no-op
  }
}
