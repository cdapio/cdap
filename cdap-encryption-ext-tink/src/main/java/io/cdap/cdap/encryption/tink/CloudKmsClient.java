/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.encryption.tink;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.ServiceOptions;
import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.kms.v1.CryptoKey.CryptoKeyPurpose;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.CryptoKeyVersion.CryptoKeyVersionAlgorithm;
import com.google.cloud.kms.v1.CryptoKeyVersionTemplate;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyRing;
import com.google.cloud.kms.v1.KeyRingName;
import com.google.cloud.kms.v1.LocationName;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import io.cdap.cdap.security.spi.encryption.CipherInitializationException;
import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client wrapping Cloud KMS.
 */
public class CloudKmsClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(CloudKmsClient.class);

  private final KeyManagementServiceClient client;
  private final String projectId;
  private final String location;
  private final String keyRingId;
  // In-memory cache to hold created crypto keys, this is to avoid checking if a given crypto key exists.
  private final Set<String> knownCryptoKeys;

  /**
   * Constructs Cloud KMS client.
   *
   * @throws IOException if cloud kms client can not be created
   */
  CloudKmsClient(String projectId, String location, String keyRingId) throws IOException {
    if (projectId != null) {
      this.projectId = projectId;
    } else {
      this.projectId = ServiceOptions.getDefaultProjectId();
    }
    this.location = location;
    this.keyRingId = keyRingId;
    this.client = KeyManagementServiceClient.create();
    this.knownCryptoKeys = new HashSet<>();
  }

  /**
   * Creates a new key ring with the given ID.
   *
   * @throws CipherInitializationException If there's an error while creating the key ring
   */
  void createKeyRingIfNotExists() throws CipherInitializationException {
    LOG.debug("Creating key ring with id {} in projects/{}/locations/{}.", keyRingId, projectId,
        location);

    try {
      client.getKeyRing(KeyRingName.of(projectId, location, keyRingId));
      // If we get here, the key ring was found, so we skip creating the key ring.
      return;
    } catch (ApiException e) {
      if (!e.getStatusCode().getCode().equals(Code.NOT_FOUND)) {
        throw new CipherInitializationException("Failed to check if Cloud KMS key ring exists", e);
      }
    }

    LocationName locationName = LocationName.of(projectId, location);
    try {
      client.createKeyRing(locationName, keyRingId, KeyRing.newBuilder().build());
    } catch (ApiException e) {
      if (!e.getStatusCode().getCode().equals(Code.ALREADY_EXISTS)) {
        throw new CipherInitializationException("Failed to create new Cloud KMS key ring", e);
      }
      LOG.info("Failed to create new Cloud KMS key ring, likely already exists.");
    }
  }

  /**
   * Creates a new crypto key with the given ID.
   *
   * @param cryptoKeyId The crypto key ID
   * @throws CipherInitializationException If there's an error creating crypto key
   */
  void createCryptoKeyIfNotExists(String cryptoKeyId) throws CipherInitializationException {
    // If crypto key is already created, do not attempt to create it again.
    if (knownCryptoKeys.contains(cryptoKeyId)) {
      return;
    }

    try {
      client.getCryptoKey(CryptoKeyName.of(projectId, location, keyRingId, cryptoKeyId));
      // If we get here, the crypto key was found, so we skip creating the crypto key.
      return;
    } catch (ApiException e) {
      if (!e.getStatusCode().getCode().equals(Code.NOT_FOUND)) {
        throw new CipherInitializationException("Failed to check if Cloud KMS crypto key exists",
            e);
      }
    }

    // Calculate the date 24 hours from now (this is used below).
    long tomorrow = Instant.now().plus(24, ChronoUnit.HOURS).getEpochSecond();

    // Build the key to create with a rotation schedule.
    com.google.cloud.kms.v1.CryptoKey key =
        com.google.cloud.kms.v1.CryptoKey.newBuilder()
            .setPurpose(CryptoKeyPurpose.ENCRYPT_DECRYPT)
            .setVersionTemplate(
                CryptoKeyVersionTemplate.newBuilder()
                    .setAlgorithm(CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION))

            // Rotate every 30 days.
            .setRotationPeriod(
                Duration.newBuilder().setSeconds(java.time.Duration.ofDays(90).getSeconds()))

            // Start the first rotation in 24 hours.
            .setNextRotationTime(Timestamp.newBuilder().setSeconds(tomorrow))
            .build();

    CryptoKey createdKey;
    // Create the key.
    try {
      createdKey = client
          .createCryptoKey(KeyRingName.of(projectId, location, keyRingId), cryptoKeyId, key);
      LOG.info("Created key with rotation schedule {}", createdKey.getName());
    } catch (ApiException e) {
      if (!e.getStatusCode().getCode().equals(Code.ALREADY_EXISTS)) {
        throw new CipherInitializationException("Failed to create crypto key", e);
      }
      LOG.info("Failed to create Cloud KMS crypto key, likely already exists.");
    }

    // In-memory cache to keep list of crypto keys created so far.
    knownCryptoKeys.add(cryptoKeyId);
  }

  @Override
  public void close() throws IOException {
    knownCryptoKeys.clear();
  }
}
