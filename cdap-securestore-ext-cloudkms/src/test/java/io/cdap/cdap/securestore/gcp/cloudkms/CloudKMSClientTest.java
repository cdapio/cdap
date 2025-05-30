/*
 * Copyright © 2025 Cask Data, Inc.
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

import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.api.services.cloudkms.v1.model.CryptoKey;
import com.google.api.services.cloudkms.v1.model.ListCryptoKeysResponse;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CloudKMSClientTest {

  private static final String TEST_PROJECT_ID = "test-project";
  private static final String TEST_KEYRING_ID = "test-instance";
  private static final String LOCATION_ID = "global";

  @Mock
  private CloudKMS mockKMS;
  @Mock
  private CloudKMS.Projects mockProjects;
  @Mock
  private CloudKMS.Projects.Locations mockLocations;
  @Mock
  private CloudKMS.Projects.Locations.KeyRings mockKeyRings;
  @Mock
  private CloudKMS.Projects.Locations.KeyRings.CryptoKeys mockCryptoKeys;
  @Mock
  private CloudKMS.Projects.Locations.KeyRings.CryptoKeys.Create mockCreate;
  @Mock
  private CloudKMS.Projects.Locations.KeyRings.CryptoKeys.Patch mockPatch;
  @Mock
  private CloudKMS.Projects.Locations.KeyRings.CryptoKeys.List mockList;

  @Captor
  private ArgumentCaptor<CryptoKey> cryptoKeyCaptor;
  @Captor
  private ArgumentCaptor<String> resourceNameCaptor;
  @Captor
  private ArgumentCaptor<String> updateMaskCaptor;

  @Before
  public void setUp() throws Exception {
    when(mockKMS.projects()).thenReturn(mockProjects);
    when(mockProjects.locations()).thenReturn(mockLocations);
    when(mockLocations.keyRings()).thenReturn(mockKeyRings);
    when(mockKeyRings.cryptoKeys()).thenReturn(mockCryptoKeys);
    when(mockCryptoKeys.create(anyString(), any(CryptoKey.class))).thenReturn(mockCreate);
    when(mockCreate.setCryptoKeyId(anyString())).thenReturn(mockCreate);
    when(mockCryptoKeys.patch(anyString(), any(CryptoKey.class))).thenReturn(mockPatch);
    when(mockPatch.setUpdateMask(anyString())).thenReturn(mockPatch);
    when(mockCryptoKeys.list(anyString())).thenReturn(mockList);
  }

  private CloudKMSClient initializeClient(boolean rotationEnabled) throws IOException {
    Map<String, String> properties = new HashMap<>();
    properties.put("project.id", TEST_PROJECT_ID);
    properties.put("keyring.id", TEST_KEYRING_ID);
    properties.put("key.rotation.period.days", "90");
    properties.put("key.rotation.enabled", String.valueOf(rotationEnabled));
    return new CloudKMSClient(properties) {
      @Override
      CloudKMS createCloudKMS(String serviceAccountFile) {
        return mockKMS;
      }
    };
  }

  @Test
  public void testCreateCryptoKeyIfNotExists_setsRotationPeriodDetails() throws IOException {
    CloudKMSClient cloudKMSclient = initializeClient(true);
    String cryptoKeyId = "new-test-key";
    Instant testStartTime = Instant.now();
    cloudKMSclient.createCryptoKeyIfNotExists(cryptoKeyId);

    verify(mockCryptoKeys).create(anyString(), cryptoKeyCaptor.capture());
    CryptoKey capturedKey = cryptoKeyCaptor.getValue();

    assertNotNull(capturedKey.getNextRotationTime());
    Instant nextRotation = Instant.parse(capturedKey.getNextRotationTime());
    assertTrue(nextRotation.isAfter(testStartTime));

    long expectedSeconds = Duration.ofDays(90).getSeconds();
    assertEquals(expectedSeconds + "s", capturedKey.getRotationPeriod());
  }

  @Test
  public void testCreateCryptoKeyIfNotExists_doesNotSetRotationWhenDisabled() throws IOException {
    CloudKMSClient cloudKMSclient = initializeClient(false);
    String cryptoKeyId = "new-key-no-rotation";
    cloudKMSclient.createCryptoKeyIfNotExists(cryptoKeyId);

    verify(mockCryptoKeys).create(anyString(), cryptoKeyCaptor.capture());
    CryptoKey capturedKey = cryptoKeyCaptor.getValue();

    assertNull(capturedKey.getNextRotationTime());
    assertNull(capturedKey.getRotationPeriod());
  }

  @Test
  public void testEnableRotationForExistingKeys_updatesKeyWithoutRotation() throws IOException {
    CloudKMSClient cloudKMSclient = initializeClient(true);
    String keyToUpdateId = "key-without-rotation";
    String keyToUpdateFullName = String.format("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
        TEST_PROJECT_ID, LOCATION_ID, TEST_KEYRING_ID, keyToUpdateId);

    String keyToSkipId = "key-with-rotation";
    String keyToSkipFullName = String.format("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
        TEST_PROJECT_ID, LOCATION_ID, TEST_KEYRING_ID, keyToSkipId);

    CryptoKey keyToUpdate = new CryptoKey().setName(keyToUpdateFullName);
    CryptoKey keyToSkip = new CryptoKey().setName(keyToSkipFullName).setRotationPeriod("7776000s");

    ListCryptoKeysResponse response = new ListCryptoKeysResponse().setCryptoKeys(
        Arrays.asList(keyToUpdate, keyToSkip));
    when(mockList.execute()).thenReturn(response);

    cloudKMSclient.enableKeyRotationIfNeeded();

    verify(mockPatch, times(1)).execute();
    verify(mockCryptoKeys).patch(resourceNameCaptor.capture(), cryptoKeyCaptor.capture());
    verify(mockPatch).setUpdateMask(updateMaskCaptor.capture());

    assertEquals(keyToUpdateFullName, resourceNameCaptor.getValue());

    long expectedSeconds = Duration.ofDays(90).getSeconds();
    assertEquals(expectedSeconds + "s", cryptoKeyCaptor.getValue().getRotationPeriod());
    assertEquals("rotation_period,next_rotation_time", updateMaskCaptor.getValue());
  }
}
