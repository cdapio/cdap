/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper on {@link CloudKMS} client.
 */
public class CloudKMSClient {
  private final CloudKMS cloudKMS;
  private final CloudKMSConf conf;
  private final List<String> crypoKeyList;

  /**
   * Constructs Cloud KMS client
   *
   * @param conf cloud kms conf used to initialize cloud kms
   * @throws IOException if cloud kms client can not be authorized or cdap key ring can not be created
   */
  public CloudKMSClient(CloudKMSConf conf) throws IOException {
    this.conf = conf;
    this.crypoKeyList = new ArrayList<>();
    this.cloudKMS = createAuthorizedClient();
    createKeyRing(conf.getKeyringId());
  }

  /**
   * Creates an authorized CloudKMS client service using Application Default Credentials.
   *
   * @return an authorized CloudKMS client
   * @throws IOException if there's an error getting the default credentials
   */
  private CloudKMS createAuthorizedClient() throws IOException {
    // Create the credential
    HttpTransport transport = new NetHttpTransport();
    JsonFactory jsonFactory = new JacksonFactory();
    GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);

    // Depending on the environment that provides the default credentials (e.g. Compute Engine, App
    // Engine), the credentials may require us to specify the scopes we need explicitly.
    // Check for this case, and inject the scope if required.
    if (credential.createScopedRequired()) {
      credential = credential.createScoped(CloudKMSScopes.all());
    }

    return new CloudKMS.Builder(transport, jsonFactory, credential)
      .setApplicationName("CloudKMS snippets")
      .build();
  }

  /**
   * Creates a new key ring with the given id.
   *
   * @param keyRingId key ring id
   * @throws IOException if there's an error creating key ring
   */
  private void createKeyRing(String keyRingId) throws IOException {
    String parent = String.format("projects/%s/locations/%s", conf.getProjectId(), conf.getLocationId());
    try {
      cloudKMS.projects().locations().keyRings()
        .create(parent, new KeyRing())
        .setKeyRingId(keyRingId)
        .execute();
    } catch (GoogleJsonResponseException e) {
      // if key ring already exists, then do not throw any exception.
      if (e.getStatusCode() != 409) {
        throw e;
      }
    }
  }

  /**
   * Creates a new crypto key on cloud kms with the given id.
   *
   * @param cryptoKeyId crypto key id
   * @throws IOException if there's an error creating crypto key
   */
  void createCryptoKey(String cryptoKeyId) throws Exception {
    // If crypto key is already created, do not attempt to create it again.
    if (crypoKeyList.contains(cryptoKeyId)) {
      return;
    }

    String parent = String.format(
      "projects/%s/locations/%s/keyRings/%s", conf.getProjectId(), conf.getLocationId(), conf.getKeyringId());

    CryptoKey cryptoKey = new CryptoKey();
    // This will allow the API access to the key for encryption and decryption. This option will enable
    // Symmetric encryption
    cryptoKey.setPurpose("ENCRYPT_DECRYPT");

    try {
      cloudKMS.projects().locations().keyRings().cryptoKeys()
        .create(parent, cryptoKey)
        .setCryptoKeyId(cryptoKeyId)
        .execute();
    } catch (GoogleJsonResponseException e) {
      // Crypto key is shared for all the namespaces. So if crypto key already exists, then do not throw any exception.
      if (e.getStatusCode() != 409) {
        throw e;
      }
    }

    // In-memory cache to keep list of crypto keys stored so far.
    crypoKeyList.add(cryptoKeyId);
  }

  /**
   * Encrypts secret with provided crypto key.
   *
   * @param cryptoKeyId crypto key to encrypt secret
   * @param secret secret to be encrypted
   * @throws IOException there's an error in encrypting secret
   */
  byte[] encrypt(String cryptoKeyId, byte[] secret) throws IOException {
    String resourceName = String.format(
      "projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
      conf.getProjectId(), conf.getLocationId(), conf.getKeyringId(), cryptoKeyId);

    EncryptRequest request = new EncryptRequest().encodePlaintext(secret);
    EncryptResponse response = cloudKMS.projects().locations().keyRings().cryptoKeys()
      .encrypt(resourceName, request)
      .execute();

    return response.decodeCiphertext();
  }

  /**
   * Decrypts the provided encrypted secret with the specified crypto key.
   *
   * @param cryptoKeyId crypto key to decrypt secret
   * @param encryptedSecret encrypted secret
   * @return decrypted secret
   * @throws IOException there's an error in decrypting secret
   */
  byte[] decrypt(String cryptoKeyId, byte[] encryptedSecret) throws IOException {
    String resourceName = String.format(
      "projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
      conf.getProjectId(), conf.getLocationId(), conf.getKeyringId(), cryptoKeyId);

    DecryptRequest request = new DecryptRequest().encodeCiphertext(encryptedSecret);
    DecryptResponse response = cloudKMS.projects().locations().keyRings().cryptoKeys()
      .decrypt(resourceName, request)
      .execute();

    return response.decodePlaintext();
  }
}
