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
import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Secrets manager which uses Google {@link CloudKMS} to encrypt and decrypt secrets. It also stores encrypted data
 * and secret's metadata in {@link SecretStore}.
 */
public class CloudSecretsManager implements SecretManager {
  private static final Logger LOG = LoggerFactory.getLogger(CloudKMSClient.class);
  private static final Gson GSON = new Gson();
  private SecretStore store;
  private CloudKMSClient client;

  @Override
  public String getName() {
    return "cloudkms";
  }

  @Override
  public void initialize(SecretManagerContext context) throws Exception {
    this.store = context.getSecretsMetadataStore();
    this.client = new CloudKMSClient();
    this.client.createKeyRing();
  }

  @Override
  public void store(String namespace, String name, byte[] secret, String description,
                    Map<String, String> properties) throws Exception {
    // creates new crypto key for every namespace
    client.createCryptoKey(namespace);
    byte[] encryptedData = client.encrypt(namespace, secret);
    byte[] encodedSecret = encode(new Secret(encryptedData, new SecretMetadata(name, description,
                                                                               System.currentTimeMillis(),
                                                                               properties)));
    store.store(namespace, name, encodedSecret);
  }

  @Override
  public Secret get(String namespace, String name) throws Exception {
    byte[] secretBytes = store.get(namespace, name);
    Secret secret = decode(secretBytes);
    byte[] decrypted = client.decrypt(namespace, secret.getData());
    return new Secret(decrypted, secret.getMetadata());
  }

  @Override
  public Collection<SecretMetadata> list(String namespace) throws Exception {
    Collection<byte[]> secrets = store.list(namespace);
    Collection<SecretMetadata> secretMetadatas = new ArrayList<>();
    for (byte[] secret : secrets) {
      Secret decodedSecret = decode(secret);
      secretMetadatas.add(decodedSecret.getMetadata());
    }

    return secretMetadatas;
  }

  @Override
  public void delete(String namespace, String name) throws Exception {
    // Cloud KMS does not provide capability to delete crypto keys. So just delete metadata associated with the
    // secret from metadata store. This should be fine because cloud kms does not store any encrypted secrets.
    // Reference: https://cloud.google.com/kms/docs/faq#cannnot_delete
    store.delete(namespace, name);
  }

  @Override
  public void destroy(SecretManagerContext context) {
    // no-op
  }

  private byte[] encode(Secret secret) {
    return GSON.toJson(secret).getBytes(StandardCharsets.UTF_8);
  }

  private Secret decode(byte[] bytes) {
    return GSON.fromJson(new String(bytes, StandardCharsets.UTF_8), Secret.class);
  }
}
