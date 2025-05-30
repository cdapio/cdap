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

package io.cdap.cdap.securestore.gcp.cloudkms;

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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper on {@link CloudKMS} client.
 */
class CloudKMSClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(CloudKMSClient.class);
  // When created in the global location, Cloud KMS resources are available from zones spread around the world.
  private static final String LOCATION_ID = "global";
  // All the crypto keys are created under keyring named cdap
  private static final String ENCRYPT_DECRYPT = "ENCRYPT_DECRYPT";
  private static final String CLOUD_KMS = "cloudkms";
  private static final String PROJECT_ID = "project.id";
  private static final String SERVICE_ACCOUNT_FILE = "service.account.file";
  private static final String ENABLE_KEY_ROTATION = "key.rotation.enabled";
  private static final String KEY_ROTATION_PERIOD_DAYS = "key.rotation.period.days";
  private static final String METADATA_SERVER_API = "metadata.server.api";
  private static final String DEFAULT_METADATA_SERVER_API =
      "http://metadata.google.internal/computeMetadata"
          + "/v1/project/project-id";
  private static final String KEYRING_ID = "keyring.id";
  private static final String DEFAULT_KEYRING_ID = "cdap";
  private static final String KEY_RING_FORMAT = "projects/%s/locations/%s/keyRings/%s";
  private static final String CRYPTO_KEY_FORMAT = KEY_RING_FORMAT + "/cryptoKeys/%s";
  private static final String DEFAULT_KEY_ROTATION_PERIOD_DAYS = "90";


  private final CloudKMS cloudKMS;
  private final String projectId;
  private final String keyringId;
  // In-memory cache to hold created crypto keys, this is to avoid checking if a given crypto key exists.
  private final Set<String> knownCryptoKeys;
  private final Boolean enableKeyRotation;
  private final Long keyRotationPeriod;

  /**
   * Constructs Cloud KMS client.
   *
   * @throws IOException if cloud kms client can not be created
   */
  CloudKMSClient(Map<String, String> properties) throws IOException {
    String metadataServerApi = properties.getOrDefault(METADATA_SERVER_API,
        DEFAULT_METADATA_SERVER_API);
    this.projectId = properties.containsKey(PROJECT_ID) ? properties.get(PROJECT_ID) :
        getSystemProjectId(metadataServerApi);
    String serviceAccountFile = properties.getOrDefault(SERVICE_ACCOUNT_FILE, null);
    this.cloudKMS = createCloudKMS(serviceAccountFile);
    this.keyringId = properties.getOrDefault(KEYRING_ID, DEFAULT_KEYRING_ID);
    this.knownCryptoKeys = new HashSet<>();
    this.enableKeyRotation = Boolean.parseBoolean(
        properties.get(ENABLE_KEY_ROTATION));
    this.keyRotationPeriod = initializeKeyRotationPeriod(properties);
  }

  private long initializeKeyRotationPeriod(Map<String, String> properties) {
    long rotationPeriod = Long.parseLong(
        properties.getOrDefault(KEY_ROTATION_PERIOD_DAYS, DEFAULT_KEY_ROTATION_PERIOD_DAYS));
    if (rotationPeriod < 1) {
      LOG.warn(
          "Configured value for '{}' is {} which is less than minimum requirement of "
              + "1 day. Falling back to the default of {} days.",
          KEY_ROTATION_PERIOD_DAYS, rotationPeriod, DEFAULT_KEY_ROTATION_PERIOD_DAYS);
      rotationPeriod = Long.parseLong(DEFAULT_KEY_ROTATION_PERIOD_DAYS);
    }
    return rotationPeriod;
  }

  /**
   * Get project id from the metadata server. Makes a request to the metadata server that lives on
   * the VM, as described at https://cloud.google.com/compute/docs/storing-retrieving-metadata.
   */
  private String getSystemProjectId(String metadataServerApi) throws IOException {
    URL url = new URL(metadataServerApi);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    try {
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestProperty("Metadata-Flavor", "Google");
      connection.connect();
      try (Reader reader = new InputStreamReader(connection.getInputStream(),
          StandardCharsets.UTF_8)) {
        return CharStreams.toString(reader);
      }
    } finally {
      connection.disconnect();
    }
  }

  /**
   * Creates an authorized CloudKMS client service.
   *
   * @return an authorized CloudKMS client
   * @throws IOException if credentials can not be created in current environment
   */
   @VisibleForTesting
   CloudKMS createCloudKMS(String serviceAccountFile) throws IOException {
    HttpTransport transport = new NetHttpTransport();
    JsonFactory jsonFactory = new JacksonFactory();
    GoogleCredential credential;

    if (serviceAccountFile == null) {
      credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
    } else {
      File credentialsPath = new File(serviceAccountFile);
      try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
        credential = GoogleCredential.fromStream(serviceAccountStream, transport, jsonFactory);
      }
    }

    if (credential.createScopedRequired()) {
      credential = credential.createScoped(CloudKMSScopes.all());
    }

    return new CloudKMS.Builder(transport, jsonFactory, credential)
        .setApplicationName(CLOUD_KMS)
        .build();
  }

  /**
   * Creates a new key ring with the given id.
   *
   * @throws IOException if there's an error while creating the key ring
   */
  void createKeyRingIfNotExists() throws IOException {
    String parent = String.format("projects/%s/locations/%s", projectId, LOCATION_ID);
    LOG.debug("Creating key ring with id {}.", keyringId);

    try {
      cloudKMS.projects().locations().keyRings()
          .create(parent, new KeyRing())
          .setKeyRingId(keyringId)
          .execute();
    } catch (GoogleJsonResponseException e) {
      // if key ring already exists, then do not throw any exception.
      if (e.getDetails() != null && e.getDetails().getCode() == 409) {
        LOG.trace(String.format("Key ring %s already exists", keyringId));
        return;
      }
      throw new IOException(
          String.format("Exception occurred while creating key ring %s", keyringId), e);
    }
  }

   void enableKeyRotationIfNeeded() throws IOException {
    if (!this.enableKeyRotation) {
      return;
    }

    String keyRingName = String.format(KEY_RING_FORMAT, projectId, LOCATION_ID, keyringId);
    try {
      List<CryptoKey> cryptoKeys = cloudKMS.projects().locations().keyRings().cryptoKeys()
          .list(keyRingName).execute().getCryptoKeys();
      if (cryptoKeys == null || cryptoKeys.isEmpty()) {
        LOG.info("No existing crypto keys found in keyring {}.", keyringId);
        return;
      }

      for (CryptoKey cryptoKey : cryptoKeys) {
        String cryptoKeyName = cryptoKey.getName();
        String cryptoKeyId = cryptoKeyName.substring(
            cryptoKeyName.lastIndexOf('/') + 1);

        if (Strings.isNullOrEmpty(cryptoKey.getRotationPeriod())) {
          CryptoKey updateRequest = new CryptoKey();
          // Setting rotation period to 90 days for KMS keys.
          long rotationInSeconds = Duration.ofDays(this.keyRotationPeriod).getSeconds();
          updateRequest.setRotationPeriod(rotationInSeconds + "s");
          // Setting nextRotationTime to 1 day after the key is updated.
          Instant nextRotationTime = Instant.now().plus(Duration.ofDays(1));
          updateRequest.setNextRotationTime(nextRotationTime.toString());

          String resourceName = String.format(CRYPTO_KEY_FORMAT, projectId, LOCATION_ID, keyringId, cryptoKeyId);
          try {
            cloudKMS.projects().locations().keyRings().cryptoKeys()
                .patch(resourceName, updateRequest)
                .setUpdateMask(
                    "rotation_period,next_rotation_time")
                .execute();
            LOG.info("Successfully updated rotation period for crypto key {}.", cryptoKeyId);
          } catch (GoogleJsonResponseException e) {
            throw new IOException(
                String.format("Failed to update rotation period for crypto key %s", cryptoKeyId),
                e);
          }
        }
      }
    } catch (GoogleJsonResponseException e) {
      throw new IOException(
          String.format("Exception occurred while listing crypto keys in keyring %s", keyringId),
          e);
    }
  }

  /**
   * Creates a new crypto key on google cloud kms with the given id.
   *
   * @param cryptoKeyId crypto key id
   * @throws IOException if there's an error creating crypto key
   */
  void createCryptoKeyIfNotExists(String cryptoKeyId) throws IOException {
    // If crypto key is already created, do not attempt to create it again.
    if (knownCryptoKeys.contains(cryptoKeyId)) {
      return;
    }

    CryptoKey cryptoKey = new CryptoKey();
    // This will allow the API access to the key for symmetric encryption and decryption.
    cryptoKey.setPurpose(ENCRYPT_DECRYPT);

    if (this.enableKeyRotation) {
      long rotationInSeconds = Duration.ofDays(this.keyRotationPeriod).getSeconds();
      cryptoKey.setRotationPeriod(rotationInSeconds + "s");

      Instant nextRotationTime = Instant.now().plus(Duration.ofDays(1));
      cryptoKey.setNextRotationTime(nextRotationTime.toString());
    }

    try {
      String parent = String.format(KEY_RING_FORMAT, projectId, LOCATION_ID, keyringId);
      cloudKMS.projects().locations().keyRings().cryptoKeys()
          .create(parent, cryptoKey)
          .setCryptoKeyId(cryptoKeyId)
          .execute();
    } catch (GoogleJsonResponseException e) {
      // Crypto key is shared for all the secrets in a namespace. If the crypto key already exists, then do not throw
      // any exception. This will happen if another key for the same namespace is being created.
      if (e.getDetails() != null && e.getDetails().getCode() == 409) {
        LOG.trace(String.format("Key %s already exists", cryptoKeyId));
        return;
      }

      throw new IOException("Error occurred while creating cryptographic key for namespace %s", e);
    }

    // In-memory cache to keep list of crypto keys created so far.
    knownCryptoKeys.add(cryptoKeyId);
  }

  /**
   * Encrypts secret with provided crypto key.
   *
   * @param cryptoKeyId crypto key to encrypt secret
   * @param secret secret to be encrypted
   * @throws IOException there's an error in encrypting secret
   */
  byte[] encrypt(String cryptoKeyId, byte[] secret) throws IOException {
    String resourceName = String.format(CRYPTO_KEY_FORMAT, projectId, LOCATION_ID,
        keyringId, cryptoKeyId);
    // secret must not be longer than 64KiB.
    EncryptRequest request = new EncryptRequest().encodePlaintext(secret);
    EncryptResponse response = cloudKMS.projects().locations().keyRings().cryptoKeys()
        .encrypt(resourceName, request)
        .execute();

    byte[] encryptedData = response.decodeCiphertext();
    if (encryptedData == null) {
      throw new IOException("Error while encrypting the secret. Encrypted data is null.");
    }

    return encryptedData;
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
    String resourceName = String.format(CRYPTO_KEY_FORMAT, projectId, LOCATION_ID, keyringId,
        cryptoKeyId);

    DecryptRequest request = new DecryptRequest().encodeCiphertext(encryptedSecret);
    DecryptResponse response = cloudKMS.projects().locations().keyRings().cryptoKeys()
        .decrypt(resourceName, request)
        .execute();

    byte[] decrypted = response.decodePlaintext();
    if (decrypted == null) {
      throw new IOException("Error while decrypting the secret. Decrypted data is null");
    }

    return decrypted;
  }

  @Override
  public void close() throws IOException {
    knownCryptoKeys.clear();
  }
}
