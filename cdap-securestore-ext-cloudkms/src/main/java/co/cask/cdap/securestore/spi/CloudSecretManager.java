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

package co.cask.cdap.securestore.spi;

import co.cask.cdap.securestore.spi.secret.Secret;
import co.cask.cdap.securestore.spi.secret.SecretMetadata;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 */
public class CloudSecretManager implements SecretManager {
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
    this.store = context.getSecretStore();
    this.client = new CloudKMSClient();
    this.client.createKeyRing();
  }

  @Override
  public void store(String namespace, Secret secret) throws Exception {
    // creates new crypto key for every namespace
    client.createCryptoKey(namespace);
    byte[] encryptedData = client.encrypt(namespace, secret.getData());
    // TODO use builder to create the object
    byte[] encodedSecret = encode(new Secret(encryptedData, new SecretMetadata(, description,
                                                                               System.currentTimeMillis(),
                                                                               properties)));
    store.store(namespace, name, encodedSecret);
  }

  @Override
  public Secret get(String namespace, String name) throws Exception {
    byte[] secretBytes = store.get(namespace, name);
    Secret secret = decode(secretBytes);
    byte[] decrypted = client.decrypt(namespace, secret.getData());
    return new Secret(decrypted, secret.getMetadata()); m
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

  }

  @Override
  public void destroy(SecretManagerContext context) {

  }
}
