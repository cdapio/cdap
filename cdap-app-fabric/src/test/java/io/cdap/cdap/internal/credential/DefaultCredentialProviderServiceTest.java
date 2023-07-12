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
import io.cdap.cdap.api.security.credential.CredentialProvisioningException;
import io.cdap.cdap.api.security.credential.IdentityValidationException;
import io.cdap.cdap.api.security.credential.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link DefaultCredentialProviderService}.
 */
public class DefaultCredentialProviderServiceTest extends CredentialProviderTestBase {

  private static DefaultCredentialProviderService credentialProviderService;

  @BeforeClass
  public static void startup() {
    credentialProviderService = new DefaultCredentialProviderService(CConfiguration.create(),
        contextAccessEnforcer, new MockCredentialProviderProvider(), credentialIdentityManager,
        credentialProfileManager);
  }

  @Test
  public void testProvisionSuccess() throws Exception {
    // Create a new profile.
    String namespace = "testProvisionSuccess";
    CredentialProfileId profileId = createDummyProfile(CREDENTIAL_PROVIDER_TYPE_SUCCESS, namespace,
        "test-profile");

    // Create a new identity.
    CredentialIdentityId id = new CredentialIdentityId(namespace, "test");
    CredentialIdentity identity = new CredentialIdentity(profileId.getNamespace(),
        profileId.getName(), "some-identity", "some-secure-value");
    credentialIdentityManager.create(id, identity);

    Assert.assertEquals(RETURNED_TOKEN, credentialProviderService.provision(namespace, "test"));
  }

  @Test(expected = NotFoundException.class)
  public void testProvisionWithNotFoundIdentityThrowsException() throws Exception {
    String namespace = "testProvisionWithNotFoundIdentityThrowsException";
    credentialProviderService.provision(namespace, "does-not-exist");
  }

  @Test(expected = CredentialProvisioningException.class)
  public void testProvisionFailureThrowsException() throws Exception {
    // Create a new profile.
    String namespace = "testProvisionFailureThrowsException";
    CredentialProfileId profileId = createDummyProfile(CREDENTIAL_PROVIDER_TYPE_PROVISION_FAILURE,
        namespace, "test-profile");

    // Create a new identity.
    CredentialIdentityId id = new CredentialIdentityId(namespace, "test");
    CredentialIdentity identity = new CredentialIdentity(profileId.getNamespace(),
        profileId.getName(), "some-identity", "some-secure-value");
    credentialIdentityManager.create(id, identity);

    Assert.assertEquals(RETURNED_TOKEN, credentialProviderService.provision(namespace, "test"));
  }

  @Test
  public void testIdentityValidationSuccess() throws Exception {
    // Create a new profile.
    String namespace = "testIdentityValidationSuccess";
    CredentialProfileId profileId = createDummyProfile(CREDENTIAL_PROVIDER_TYPE_SUCCESS,
        namespace, "test-profile");

    CredentialIdentity identity = new CredentialIdentity(profileId.getNamespace(),
        profileId.getName(), "some-identity", "some-secure-value");
    credentialProviderService.validateIdentity(identity);
  }

  @Test(expected = IdentityValidationException.class)
  public void testIdentityValidationOnProvisionFailureThrowsException() throws Exception {
    // Create a new profile.
    String namespace = "testIdentityValidationFailureThrowsException";
    CredentialProfileId profileId = createDummyProfile(CREDENTIAL_PROVIDER_TYPE_PROVISION_FAILURE,
        namespace, "test-profile");

    CredentialIdentity identity = new CredentialIdentity(profileId.getNamespace(),
        profileId.getName(), "some-identity", "some-secure-value");
    credentialProviderService.validateIdentity(identity);
  }

  @Test(expected = NotFoundException.class)
  public void testIdentityValidationWithNotFoundProfileThrowsException() throws Exception {
    String namespace = "testIdentityValidationWithNotFoundProfileThrowsException";
    CredentialIdentity identity = new CredentialIdentity(namespace, "does-not-exist",
        "some-identity", "some-secure-value");
    credentialProviderService.validateIdentity(identity);
  }
}
