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

import io.cdap.cdap.api.security.credential.CredentialIdentity;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link CredentialIdentityManager}.
 */
public class CredentialIdentityManagerTest extends CredentialProviderTestBase {

  private void assertCredentialIdentitiesEqual(CredentialIdentity expected,
      CredentialIdentity actual) {
    Assert.assertEquals(expected.getProfileNamespace(), actual.getProfileNamespace());
    Assert.assertEquals(expected.getProfileName(), actual.getProfileName());
    Assert.assertEquals(expected.getIdentity(), actual.getIdentity());
    Assert.assertEquals(expected.getSecureValue(), actual.getSecureValue());
  }

  @Test
  public void testListIdentities() throws Exception {
    // Create a profile.
    String namespace = "testListIdentities";
    CredentialProfileId profileId = createDummyProfile(CREDENTIAL_PROVIDER_TYPE_SUCCESS,
        namespace, "list-identities-profile");
    // Create 2 identities.
    CredentialIdentityId id1 = new CredentialIdentityId(namespace, "list1");
    CredentialIdentity identity1 = new CredentialIdentity(profileId.getNamespace(),
        profileId.getName(), "some-identity", "some-secure-value");
    CredentialIdentityId id2 = new CredentialIdentityId(namespace, "list2");
    CredentialIdentity identity2 = new CredentialIdentity(profileId.getNamespace(),
        profileId.getName(), "some-other-identity", "some-other-secure-value");
    credentialIdentityManager.create(id1, identity1);
    credentialIdentityManager.create(id2, identity2);
    Collection<CredentialIdentityId> returnedIdentities = credentialIdentityManager
        .list(namespace);
    Assert.assertEquals(Arrays.asList(id1, id2), returnedIdentities);
  }

  @Test
  public void testCreateGetUpdateGetDelete() throws Exception {
    // Create a new profile.
    String namespace = "testCreateGetUpdateGetDelete";
    CredentialProfileId profileId = createDummyProfile(CREDENTIAL_PROVIDER_TYPE_SUCCESS,
        namespace, "test-profile");

    // Create a new identity.
    CredentialIdentityId id = new CredentialIdentityId(namespace, "test");
    CredentialIdentity identity = new CredentialIdentity(profileId.getNamespace(),
        profileId.getName(), "some-identity", "some-secure-value");
    credentialIdentityManager.create(id, identity);
    Optional<CredentialIdentity> returnedIdentity = credentialIdentityManager.get(id);
    Assert.assertTrue(returnedIdentity.isPresent());
    assertCredentialIdentitiesEqual(identity, returnedIdentity.get());

    // Update the identity.
    CredentialIdentity identity2 = new CredentialIdentity(profileId.getNamespace(),
        profileId.getName(), "some-other-identity", "some-other-secure-value");
    credentialIdentityManager.update(id, identity2);
    returnedIdentity = credentialIdentityManager.get(id);
    Assert.assertTrue(returnedIdentity.isPresent());
    assertCredentialIdentitiesEqual(identity2, returnedIdentity.get());

    // Delete the identity.
    credentialIdentityManager.delete(id);
    Optional<CredentialIdentity> emptyIdentity = credentialIdentityManager.get(id);
    Assert.assertFalse(emptyIdentity.isPresent());
  }

  @Test(expected = AlreadyExistsException.class)
  public void testCreateThrowsExceptionWhenAlreadyExists() throws Exception {
    String namespace = "testCreateThrowsExceptionWhenAlreadyExists";
    CredentialProfileId profileId = createDummyProfile(CREDENTIAL_PROVIDER_TYPE_SUCCESS,
        namespace, "test-profile");
    CredentialIdentityId id = new CredentialIdentityId(namespace, "test");
    CredentialIdentity identity1 = new CredentialIdentity(profileId.getNamespace(),
        profileId.getName(), "some-identity", "some-secure-value");
    CredentialIdentity identity2 = new CredentialIdentity(profileId.getNamespace(),
        profileId.getName(), "some-other-identity", "some-other-secure-value");
    credentialIdentityManager.create(id, identity1);
    credentialIdentityManager.create(id, identity2);
  }

  @Test(expected = NotFoundException.class)
  public void testCreateThrowsExceptionWhenProfileDoesNotExist() throws Exception {
    String namespace = "testCreateThrowsExceptionWhenProfileDoesNotExist";
    CredentialProfileId nonexistentProfile = new CredentialProfileId(namespace,
        "does-not-exist");
    CredentialIdentityId id = new CredentialIdentityId(namespace, "test");
    CredentialIdentity identity = new CredentialIdentity(nonexistentProfile.getNamespace(),
        nonexistentProfile.getName(), "some-identity", "some-secure-value");
    credentialIdentityManager.create(id, identity);
  }

  @Test(expected = NotFoundException.class)
  public void testUpdateThrowsExceptionWhenNotFound() throws Exception {
    String namespace = "testUpdateThrowsExceptionWhenNotFound";
    CredentialProfileId profileId = createDummyProfile(CREDENTIAL_PROVIDER_TYPE_SUCCESS,
        namespace, "test-profile");
    // Create a new identity.
    CredentialIdentityId id = new CredentialIdentityId(namespace, "does-not-exist");
    CredentialIdentity identity = new CredentialIdentity(profileId.getNamespace(),
        profileId.getName(), "some-identity", "some-secure-value");
    credentialIdentityManager.update(id, identity);
  }

  @Test(expected = NotFoundException.class)
  public void testDeleteThrowsExceptionWhenNotFound() throws Exception {
    String namespace = "testDeleteThrowsExceptionWhenNotFound";
    CredentialIdentityId id = new CredentialIdentityId(namespace, "does-not-exist");
    credentialIdentityManager.delete(id);
  }
}
