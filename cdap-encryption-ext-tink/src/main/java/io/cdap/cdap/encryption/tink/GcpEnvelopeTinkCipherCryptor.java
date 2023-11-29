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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.aead.AeadParameters;
import com.google.crypto.tink.aead.KmsEnvelopeAead;
import com.google.crypto.tink.aead.PredefinedAeadParameters;
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;
import io.cdap.cdap.security.spi.encryption.AeadCipherContext;
import io.cdap.cdap.security.spi.encryption.CipherInitializationException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tink cipher that allows encrypting and decrypting data using Cloud KMS.
 */
public class GcpEnvelopeTinkCipherCryptor extends AbstractTinkAeadCipherCryptor {

  /**
   * KEK URI for use by Tink. For details, see
   * https://cloud.google.com/kms/docs/client-side-encryption#connect_tink_and_cloud_kms.
   */
  private static final String TINK_GCP_KMS_KEK_URI_FORMAT
      = "gcp-kms://projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s";
  private static final String TINK_GCP_KMS_PROJECT_ID = "tink.gcp.kms.project.id";
  private static final String TINK_GCP_KMS_LOCATION = "tink.gcp.kms.location";
  private static final String TINK_GCP_KMS_KEYRING_ID = "tink.gcp.kms.keyring.id";
  private static final String TINK_GCP_KMS_CRYPTOKEY_ID = "tink.gcp.kms.cryptokey.id";
  /**
   * The Tink algorithm type. Defaults to AES256_GCM. For supported key templates, see
   * https://google.github.io/tink/javadoc/tink/1.3.0/com/google/crypto/tink/aead/AeadKeyTemplates.html.
   */
  private static final String TINK_GCP_KMS_KEY_ALGORITHM_KEY = "tink.gcp.kms.key.algorithm";

  private CloudKmsClient cloudKmsClient;

  @Override
  public String getName() {
    return "tink-gcpkms";
  }

  @Override
  public Aead initializeAead(AeadCipherContext context) throws CipherInitializationException {
    validateProperties(context);
    String projectId = context.getProperties().get(TINK_GCP_KMS_PROJECT_ID);
    String location = context.getProperties().get(TINK_GCP_KMS_LOCATION);
    String keyRingId = context.getProperties().get(TINK_GCP_KMS_KEYRING_ID);
    String cryptoKeyId = context.getProperties().get(TINK_GCP_KMS_CRYPTOKEY_ID);
    try {
      cloudKmsClient = new CloudKmsClient(projectId, location, keyRingId);
    } catch (IOException e) {
      throw new CipherInitializationException("Failed to initialize Cloud KMS client", e);
    }
    // Create KMS KeyRing and CryptoKey if they do not exist.
    cloudKmsClient.createKeyRingIfNotExists();
    cloudKmsClient.createCryptoKeyIfNotExists(cryptoKeyId);
    try {
      // Initialise Tink: register all AEAD key types with the Tink runtime
      AeadConfig.register();
    } catch (GeneralSecurityException e) {
      throw new CipherInitializationException("Failed to register Tink AEAD config", e);
    }
    String kekUri = String
        .format(TINK_GCP_KMS_KEK_URI_FORMAT, projectId, location, keyRingId, cryptoKeyId);
    Aead remoteAead;
    try {
      KmsClient client = new GcpKmsClient()
          .withCredentials(GoogleCredentials.getApplicationDefault());
      remoteAead = client.getAead(kekUri);
    } catch (GeneralSecurityException | IOException e) {
      throw new CipherInitializationException("Failed to create GCP KMS client AEAD", e);
    }
    try {
      return KmsEnvelopeAead.create(getAeadParameters(context.getProperties()), remoteAead);
    } catch (GeneralSecurityException e) {
      throw new CipherInitializationException("Failed to create Tink AEAD cipher", e);
    }
  }

  private void validateProperties(AeadCipherContext context) throws CipherInitializationException {
    Map<String, String> properties = context.getProperties();
    List<String> invalidProperties = new ArrayList<>();
    String location = properties.get(TINK_GCP_KMS_LOCATION);
    if (location == null || location.isEmpty()) {
      invalidProperties.add(TINK_GCP_KMS_LOCATION);
    }
    String keyRingId = properties.get(TINK_GCP_KMS_KEYRING_ID);
    if (keyRingId == null || keyRingId.isEmpty()) {
      invalidProperties.add(TINK_GCP_KMS_KEYRING_ID);
    }
    String cryptoKeyId = properties.get(TINK_GCP_KMS_CRYPTOKEY_ID);
    if (cryptoKeyId == null || cryptoKeyId.isEmpty()) {
      invalidProperties.add(TINK_GCP_KMS_CRYPTOKEY_ID);
    }

    if (!invalidProperties.isEmpty()) {
      throw new CipherInitializationException(
          String.format("The following properties must not be null or empty: %s",
              invalidProperties.toArray()));
    }
  }

  private AeadParameters getAeadParameters(Map<String, String> properties)
      throws CipherInitializationException {
    String key = properties.getOrDefault(TINK_GCP_KMS_KEY_ALGORITHM_KEY, "AES256_GCM");
    switch (key) {
      case "AES128_GCM":
        return PredefinedAeadParameters.AES128_GCM;
      case "AES256_GCM":
        return PredefinedAeadParameters.AES256_GCM;
      case "AES128_EAX":
        return PredefinedAeadParameters.AES128_EAX;
      case "AES256_EAX":
        return PredefinedAeadParameters.AES256_EAX;
      case "AES128_CTR_HMAC_SHA256":
        return PredefinedAeadParameters.AES128_CTR_HMAC_SHA256;
      case "AES256_CTR_HMAC_SHA256":
        return PredefinedAeadParameters.AES256_CTR_HMAC_SHA256;
      case "CHACHA20_POLY1305":
        return PredefinedAeadParameters.CHACHA20_POLY1305;
      case "XCHACHA20_POLY1305":
        return PredefinedAeadParameters.XCHACHA20_POLY1305;
      default:
        throw new CipherInitializationException(
            String.format("Unexpected AEAD algorithm: '%s'", key));
    }
  }
}
