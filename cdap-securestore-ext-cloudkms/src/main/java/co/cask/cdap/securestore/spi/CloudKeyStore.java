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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.api.services.cloudkms.v1.CloudKMSScopes;
import com.google.api.services.cloudkms.v1.model.CryptoKey;
import com.google.api.services.cloudkms.v1.model.DecryptRequest;
import com.google.api.services.cloudkms.v1.model.DecryptResponse;
import com.google.api.services.cloudkms.v1.model.EncryptRequest;
import com.google.api.services.cloudkms.v1.model.EncryptResponse;
import com.google.api.services.cloudkms.v1.model.KeyRing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class CloudKeyStore implements SecureDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(CloudKeyStore.class);
  private static final String PROJECT_ID = "cdap-dogfood";
  private static final String LOCATION = "global";
  private static final String KEYRING_ID = "cdap";
  private CloudKMS cloudKMSClient;
  private SecureDataStore store;


  @Override
  public String getType() {
    return "cloudkms";
  }

  @Override
  public void initialize(SecureDataManagerContext context) throws Exception {
    cloudKMSClient = createAuthorizedClient();
    // TODO get projectId from the context
    createKeyRing(PROJECT_ID, LOCATION, KEYRING_ID);
    store = new MockStore();
  }

  @Override
  public void storeSecureData(String namespace, String name, byte[] data, String description,
                              Map<String, String> properties) throws Exception {
    // create crypto key if does not exist. It is 1 per namespace
    createCryptoKey(PROJECT_ID, LOCATION, KEYRING_ID, namespace);
    // get encrypted data
    byte[] encryptedData = encrypt(PROJECT_ID, LOCATION, KEYRING_ID, namespace, data);
    // TODO Handle case where key already exists.
    store.storeSecureData(getKey(namespace, name),
                          new SecureData(encryptedData, new SecureDataMetadata(name, description,
                                                                               System.currentTimeMillis(),
                                                                               properties)));
  }

  @Override
  public Optional<SecureData> getSecureData(String namespace, String name) throws Exception {
    SecureData secureData = store.getSecureData(getKey(namespace, name));
    byte[] decrypted = decrypt(PROJECT_ID, LOCATION, KEYRING_ID, namespace, secureData.getData());
    return Optional.of(new SecureData(decrypted, secureData.getMetadata()));
  }

  @Override
  public Collection<SecureData> getSecureData(String namespace) throws Exception {
    // no-op
    return null;
  }

  @Override
  public void deleteSecureData(String namespace, String name) throws Exception {
    // no-op
  }

  private String getKey(String namespace, String name) {
    return namespace + ":" + name;
  }

  /**
   * Creates an authorized CloudKMS client service using Application Default Credentials.
   *
   * @return an authorized CloudKMS client
   * @throws IOException if there's an error getting the default credentials.
   */
  private CloudKMS createAuthorizedClient() throws IOException {
    // Create the credential
    HttpTransport transport = new NetHttpTransport();
    JsonFactory jsonFactory = new JacksonFactory();
    // Authorize the client using Application Default Credentials
    // @see https://g.co/dv/identity/protocols/application-default-credentials
    GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);

    // Depending on the environment that provides the default credentials (e.g. Compute Engine, App
    // Engine), the credentials may require us to specify the scopes we need explicitly.
    // Check for this case, and inject the scope if required.
    if (credential.createScopedRequired()) {
      credential = credential.createScoped(CloudKMSScopes.all());
    }

    LOG.info("Got Cloud kms client");

    return new CloudKMS.Builder(transport, jsonFactory, credential)
      .setApplicationName("CloudKMS snippets")
      .build();
  }

  /**
   * Creates a new key ring with the given id.
   */
  private void createKeyRing(String projectId, String locationId, String keyRingId) throws IOException {
    // The resource name of the location associated with the KeyRing.
    String parent = String.format("projects/%s/locations/%s", projectId, locationId);
    try {
      // Create the KeyRing for your project.
      KeyRing keyring = cloudKMSClient.projects().locations().keyRings()
        .create(parent, new KeyRing())
        .setKeyRingId(keyRingId)
        .execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == 409) {
        LOG.debug("Key ring {} already exists.", keyRingId);
      }
    }

    LOG.info("######## keyring: {}", keyRingId);
  }

  /**
   * Creates a new crypto key with the given id.
   */
  private void createCryptoKey(String projectId, String locationId, String keyRingId,
                                    String cryptoKeyId) throws Exception {
    // The resource name of the location associated with the KeyRing.
    String parent = String.format(
      "projects/%s/locations/%s/keyRings/%s", projectId, locationId, keyRingId);

    // This will allow the API access to the key for encryption and decryption. This option will enable
    // Symmetric encryption
    String purpose = "ENCRYPT_DECRYPT";
    CryptoKey cryptoKey = new CryptoKey();
    cryptoKey.setPurpose(purpose);

    try {
      // Create the CryptoKey for your project.
      CryptoKey createdKey = cloudKMSClient.projects().locations().keyRings().cryptoKeys()
        .create(parent, cryptoKey)
        .setCryptoKeyId(cryptoKeyId)
        .execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == 409) {
        LOG.debug("Crypto key {} already exists.", cryptoKeyId);
      }
    }

    LOG.info("######### Created crypto key: {}", cryptoKey);
  }

  private byte[] encrypt(String projectId, String locationId, String keyRingId, String cryptoKeyId, byte[] plaintext)
    throws IOException {
    // The resource name of the cryptoKey
    String resourceName = String.format(
      "projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
      projectId, locationId, keyRingId, cryptoKeyId);


    EncryptRequest request = new EncryptRequest().encodePlaintext(plaintext);
    EncryptResponse response = cloudKMSClient.projects().locations().keyRings().cryptoKeys()
      .encrypt(resourceName, request)
      .execute();

    return response.decodeCiphertext();
  }

  /**
   * Decrypts the provided ciphertext with the specified crypto key.
   */
  private byte[] decrypt(String projectId, String locationId, String keyRingId, String cryptoKeyId, byte[] ciphertext)
    throws IOException {
    // Create the Cloud KMS client.
    CloudKMS kms = createAuthorizedClient();

    // The resource name of the cryptoKey
    String cryptoKeyName = String.format(
      "projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
      projectId, locationId, keyRingId, cryptoKeyId);

    DecryptRequest request = new DecryptRequest().encodeCiphertext(ciphertext);
    DecryptResponse response = kms.projects().locations().keyRings().cryptoKeys()
      .decrypt(cryptoKeyName, request)
      .execute();

    return response.decodePlaintext();
  }
}
