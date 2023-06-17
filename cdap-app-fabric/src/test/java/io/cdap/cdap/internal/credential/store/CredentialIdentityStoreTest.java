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

package io.cdap.cdap.internal.credential.store;

import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.CredentialProfile;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link CredentialIdentityStore}.
 */
public class CredentialIdentityStoreTest extends CredentialStoreTestBase {

  private void assertCredentialIdentitiesEqual(CredentialIdentity expected,
      CredentialIdentity actual) {
    Assert.assertEquals(expected.getCredentialProfile(), actual.getCredentialProfile());
    Assert.assertEquals(expected.getIdentity(), actual.getIdentity());
    Assert.assertEquals(expected.getSecureValue(), actual.getSecureValue());
  }

  private CredentialProfileId createDummyProfile(String namespace, String name) throws Exception {
    CredentialProfile profile = new CredentialProfile("test", "some description",
        Collections.singletonMap("some-key", "some-value"));
    CredentialProfileId profileId = new CredentialProfileId(namespace, name);
    credentialProfileStore.create(profileId, profile);
    return profileId;
  }

  @Test
  public void testListIdentities() throws Exception {
    // Create a profile.
    String namespace = "testListIdentities";
    CredentialProfileId profileId = createDummyProfile(namespace, "list-identities-profile");
    // Create 2 identities.
    CredentialIdentityId id1 = new CredentialIdentityId(namespace, "list1");
    CredentialIdentity identity1 = new CredentialIdentity(profileId, "some-identity",
        "some-secure-value");
    CredentialIdentityId id2 = new CredentialIdentityId(namespace, "list2");
    CredentialIdentity identity2 = new CredentialIdentity(profileId, "some-other-identity",
        "some-other-secure-value");
    credentialIdentityStore.create(id1, identity1);
    credentialIdentityStore.create(id2, identity2);
    Collection<CredentialIdentityId> returnedIdentities = credentialIdentityStore
        .list(namespace);
    Assert.assertEquals(Arrays.asList(id1, id2), returnedIdentities);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateInvalidName() throws Exception {
    String namespace = "testCreateInvalidName";
    CredentialProfileId profileId = createDummyProfile(namespace, "test-profile");
    CredentialIdentityId id = new CredentialIdentityId(namespace, "_invalid");
    CredentialIdentity identity = new CredentialIdentity(profileId, "some-identity",
        "some-secure-value");
    credentialIdentityStore.create(id, identity);
  }

  @Test
  public void testCreateGetUpdateGetDelete() throws Exception {
    // Create a new profile.
    String namespace = "testCreateGetUpdateGetDelete";
    CredentialProfileId profileId = createDummyProfile(namespace, "test-profile");

    // Create a new identity.
    CredentialIdentityId id = new CredentialIdentityId(namespace, "test");
    CredentialIdentity identity = new CredentialIdentity(profileId, "some-identity",
        "some-secure-value");
    credentialIdentityStore.create(id, identity);
    Optional<CredentialIdentity> returnedIdentity = credentialIdentityStore.get(id);
    Assert.assertTrue(returnedIdentity.isPresent());
    assertCredentialIdentitiesEqual(identity, returnedIdentity.get());

    // Update the identity.
    CredentialIdentity identity2 = new CredentialIdentity(profileId, "some-other-identity",
        "some-other-secure-value");
    credentialIdentityStore.update(id, identity2);
    returnedIdentity = credentialIdentityStore.get(id);
    Assert.assertTrue(returnedIdentity.isPresent());
    assertCredentialIdentitiesEqual(identity2, returnedIdentity.get());

    // Delete the identity.
    credentialIdentityStore.delete(id);
    Optional<CredentialIdentity> emptyIdentity = credentialIdentityStore.get(id);
    Assert.assertFalse(emptyIdentity.isPresent());
  }

  @Test(expected = AlreadyExistsException.class)
  public void testCreateThrowsExceptionWhenAlreadyExists() throws Exception {
    String namespace = "testCreateThrowsExceptionWhenAlreadyExists";
    CredentialProfileId profileId = createDummyProfile(namespace, "test-profile");
    CredentialIdentityId id = new CredentialIdentityId(namespace, "test");
    CredentialIdentity identity1 = new CredentialIdentity(profileId, "some-identity",
        "some-secure-value");
    CredentialIdentity identity2 = new CredentialIdentity(profileId, "some-other-identity",
        "some-other-secure-value");
    credentialIdentityStore.create(id, identity1);
    credentialIdentityStore.create(id, identity2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateThrowsExceptionWhenProfileDoesNotExist() throws Exception {
    String namespace = "testCreateThrowsExceptionWhenProfileDoesNotExist";
    CredentialProfileId nonexistentProfile = new CredentialProfileId(namespace,
        "does-not-exist");
    CredentialIdentityId id = new CredentialIdentityId(namespace, "test");
    CredentialIdentity identity = new CredentialIdentity(nonexistentProfile, "some-identity",
        "some-secure-value");
    credentialIdentityStore.create(id, identity);
  }

  @Test(expected = NotFoundException.class)
  public void testUpdateThrowsExceptionWhenNotFound() throws Exception {
    String namespace = "testUpdateThrowsExceptionWhenNotFound";
    CredentialProfileId profileId = createDummyProfile(namespace, "test-profile");
    // Create a new identity.
    CredentialIdentityId id = new CredentialIdentityId(namespace, "does-not-exist");
    CredentialIdentity identity = new CredentialIdentity(profileId, "some-identity",
        "some-secure-value");
    credentialIdentityStore.update(id, identity);
  }

  @Test(expected = NotFoundException.class)
  public void testDeleteThrowsExceptionWhenNotFound() throws Exception {
    String namespace = "testDeleteThrowsExceptionWhenNotFound";
    CredentialIdentityId id = new CredentialIdentityId(namespace, "does-not-exist");
    credentialIdentityStore.delete(id);
  }
}
