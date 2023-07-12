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

package io.cdap.cdap.internal.credential;

import io.cdap.cdap.proto.credential.CredentialProfile;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.internal.credential.store.CredentialProfileStore;
import io.cdap.cdap.api.security.credential.CredentialIdentity;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link CredentialProfileStore}.
 */
public class CredentialProfileManagerTest extends CredentialProviderTestBase {

  private void assertCredentialProfilesEqual(CredentialProfile expected, CredentialProfile actual) {
    Assert.assertEquals(expected.getCredentialProviderType(), actual.getCredentialProviderType());
    Assert.assertEquals(expected.getDescription(), actual.getDescription());
    Assert.assertEquals(expected.getProperties(), actual.getProperties());
  }

  @Test
  public void testListProfiles() throws Exception {
    String namespace = "testListProfiles";
    // Create 2 profiles.
    CredentialProfileId id1 = new CredentialProfileId(namespace, "list1");
    CredentialProfile profile1 = new CredentialProfile(CREDENTIAL_PROVIDER_TYPE_SUCCESS,
        "some description", Collections.singletonMap("some-key", "some-value"));
    CredentialProfileId id2 = new CredentialProfileId(namespace, "list2");
    CredentialProfile profile2 = new CredentialProfile(
        CREDENTIAL_PROVIDER_TYPE_SUCCESS,
        "some other description", Collections.singletonMap("some-other-key", "some-other-value"));
    credentialProfileManager.create(id1, profile1);
    credentialProfileManager.create(id2, profile2);
    Collection<CredentialProfileId> returnedProfiles = credentialProfileManager
        .list(namespace);
    Assert.assertEquals(Arrays.asList(id1, id2), returnedProfiles);
  }

  @Test
  public void testCreateGetUpdateGetDelete() throws Exception {
    String namespace = "testCreateGetUpdateGetDelete";

    // Create a new profile.
    CredentialProfileId id = new CredentialProfileId(namespace, "test1");
    CredentialProfile profile = new CredentialProfile(CREDENTIAL_PROVIDER_TYPE_SUCCESS,
        "some description", Collections.singletonMap("some-key", "some-value"));
    credentialProfileManager.create(id, profile);
    Optional<CredentialProfile> returnedProfile = credentialProfileManager.get(id);
    Assert.assertTrue(returnedProfile.isPresent());
    assertCredentialProfilesEqual(profile, returnedProfile.get());

    // Update the profile.
    CredentialProfile profile2 = new CredentialProfile(CREDENTIAL_PROVIDER_TYPE_SUCCESS,
        "some other description", Collections.singletonMap("some-other-key", "some-other-value"));
    credentialProfileManager.update(id, profile2);
    returnedProfile = credentialProfileManager.get(id);
    Assert.assertTrue(returnedProfile.isPresent());
    assertCredentialProfilesEqual(profile2, returnedProfile.get());

    // Delete the profile.
    credentialProfileManager.delete(id);
    Optional<CredentialProfile> emptyProfile = credentialProfileManager.get(id);
    Assert.assertFalse(emptyProfile.isPresent());
  }

  @Test(expected = AlreadyExistsException.class)
  public void testCreateThrowsExceptionWhenAlreadyExists() throws Exception {
    String namespace = "testCreateThrowsExceptionWhenAlreadyExists";
    CredentialProfileId id = new CredentialProfileId(namespace, "test2");
    CredentialProfile profile = new CredentialProfile(CREDENTIAL_PROVIDER_TYPE_SUCCESS,
        "some description",
        Collections.singletonMap("some-key", "some-value"));
    CredentialProfile profile2 = new CredentialProfile(
        CREDENTIAL_PROVIDER_TYPE_SUCCESS,
        "some other description",
        Collections.singletonMap("some-other-key", "some-other-value"));
    credentialProfileManager.create(id, profile);
    credentialProfileManager.create(id, profile2);
  }

  @Test(expected = NotFoundException.class)
  public void testUpdateThrowsExceptionWhenNotFound() throws Exception {
    String namespace = "testUpdateThrowsExceptionWhenNotFound";
    CredentialProfileId id = new CredentialProfileId(namespace, "does-not-exist");
    CredentialProfile profile = new CredentialProfile(CREDENTIAL_PROVIDER_TYPE_SUCCESS,
        "some description",
        Collections.singletonMap("some-key", "some-value"));
    credentialProfileManager.update(id, profile);
  }

  @Test(expected = NotFoundException.class)
  public void testDeleteThrowsExceptionWhenNotFound() throws Exception {
    String namespace = "testDeleteThrowsExceptionWhenNotFound";
    CredentialProfileId id = new CredentialProfileId(namespace, "does-not-exist");
    credentialProfileManager.delete(id);
  }

  @Test(expected = ConflictException.class)
  public void testDeleteThrowsExceptionWhenIdentitiesStillExist() throws Exception {
    String namespace = "testDeleteThrowsExceptionWhenIdentitiesStillExist";
    CredentialProfileId profileId = new CredentialProfileId(namespace, "has-identities");
    CredentialProfile profile = new CredentialProfile(CREDENTIAL_PROVIDER_TYPE_SUCCESS,
        "some description",
        Collections.singletonMap("some-key", "some-value"));
    CredentialIdentityId identityId = new CredentialIdentityId(namespace, "test-identity");
    CredentialIdentity identity = new CredentialIdentity(profileId.getNamespace(),
        profileId.getName(), "some-identity", "some-secure-value");
    credentialProfileManager.create(profileId, profile);
    credentialIdentityManager.create(identityId, identity);
    credentialProfileManager.delete(profileId);
  }

  @Test(expected = BadRequestException.class)
  public void testCreateThrowsExceptionWhenUnsupportedProviderType() throws Exception {
    String namespace = "testCreateThrowsExceptionWhenUnsupportedProviderType";
    CredentialProfileId id = new CredentialProfileId(namespace, "test");
    CredentialProfile profile = new CredentialProfile("invalid",
        "some description", Collections.singletonMap("some-key", "some-value"));
    credentialProfileManager.create(id, profile);
  }

  @Test(expected = BadRequestException.class)
  public void testUpdateThrowsExceptionWhenUnsupportedProviderType() throws Exception {
    String namespace = "testUpdateThrowsExceptionWhenUnsupportedProviderType";
    CredentialProfileId id = new CredentialProfileId(namespace, "test");
    CredentialProfile profile = new CredentialProfile(CREDENTIAL_PROVIDER_TYPE_SUCCESS,
        "some description", Collections.singletonMap("some-key", "some-value"));
    credentialProfileManager.create(id, profile);
    CredentialProfile profile2 = new CredentialProfile("invalid",
        "some description", Collections.singletonMap("some-key", "some-value"));
    credentialProfileManager.update(id, profile2);
  }

  @Test(expected = BadRequestException.class)
  public void testCreateValidationFailureThrowsException() throws Exception {
    String namespace = "testCreateValidationFailureThrowsException";
    CredentialProfileId id = new CredentialProfileId(namespace, "test2");
    CredentialProfile profile = new CredentialProfile(CREDENTIAL_PROVIDER_TYPE_VALIDATION_FAILURE,
        "some description",
        Collections.singletonMap("some-key", "some-value"));
    credentialProfileManager.create(id, profile);
  }
}
