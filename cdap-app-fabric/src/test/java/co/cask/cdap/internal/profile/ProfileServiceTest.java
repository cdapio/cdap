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

package co.cask.cdap.internal.profile;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProfileConflictException;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.provisioner.ProvisionerInfo;
import co.cask.cdap.proto.provisioner.ProvisionerPropertyValue;
import co.cask.cdap.runtime.spi.profile.ProfileStatus;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Unit test for profile store
 */
public class ProfileServiceTest {
  private static final List<ProvisionerPropertyValue> PROPERTY_SUMMARIES =
    ImmutableList.<ProvisionerPropertyValue>builder()
      .add(new ProvisionerPropertyValue("1st property", "1st value", false))
      .add(new ProvisionerPropertyValue("2nd property", "2nd value", true))
      .add(new ProvisionerPropertyValue("3rd property", "3rd value", false))
      .build();

  private static ProfileService profileService;

  @BeforeClass
  public static void setup() throws Exception {
    profileService = AppFabricTestHelper.getInjector().getInstance(ProfileService.class);
  }

  @Test
  public void testProfileService() throws Exception {
    // get non-existing profile
    try {
      profileService.getProfile(NamespaceId.DEFAULT.profile("nonExisting"));
      Assert.fail();
    } catch (NotFoundException e) {
      // expected
    }

    // delete non-existing profile
    try {
      profileService.deleteProfile(NamespaceId.DEFAULT.profile("nonExisting"));
      Assert.fail();
    } catch (NotFoundException e) {
      // expected
    }

    ProfileId profileId = NamespaceId.DEFAULT.profile("MyProfile");
    Profile expected = new Profile("MyProfile", "my profile for testing",
      new ProvisionerInfo("defaultProvisioner", PROPERTY_SUMMARIES));
    // add a profile
    profileService.saveProfile(profileId, expected);

    // get the profile
    Assert.assertEquals(expected, profileService.getProfile(profileId));

    // add a profile which already exists, should succeed and the profile property should be updated
    expected = new Profile("MyProfile", "my 2nd profile for updating",
      new ProvisionerInfo("anotherProvisioner", Collections.emptyList()));
    profileService.saveProfile(profileId, expected);
    Assert.assertEquals(expected, profileService.getProfile(profileId));

    // add another profile to default namespace
    ProfileId profileId2 = NamespaceId.DEFAULT.profile("MyProfile2");
    Profile profile2 = new Profile("MyProfile2", "my 2nd profile for testing",
      new ProvisionerInfo("anotherProvisioner", PROPERTY_SUMMARIES));
    profileService.saveProfile(profileId2, profile2);

    // add default profile
    profileService.saveProfile(ProfileId.DEFAULT, Profile.DEFAULT);

    // get all profiles
    List<Profile> profiles = ImmutableList.of(expected, profile2, Profile.DEFAULT);
    Assert.assertEquals(profiles, profileService.getProfiles(NamespaceId.DEFAULT, true));

    // by default the profile status should be enabled
    Assert.assertEquals(ProfileStatus.ENABLED, profileService.getProfile(profileId).getStatus());
    Assert.assertEquals(ProfileStatus.ENABLED, profileService.getProfile(profileId2).getStatus());

    // by default the profile will be enabled, so enable it will throw a ProfileConflictException
    try {
      profileService.enableProfile(profileId);
      Assert.fail();
    } catch (ProfileConflictException e) {
      // expected
    }

    // disable the profile should success
    profileService.disableProfile(profileId);

    // check the profile status to the disabled
    Assert.assertEquals(ProfileStatus.DISABLED, profileService.getProfile(profileId).getStatus());

    // disable again should throw ProfileConflictException
    try {
      profileService.disableProfile(profileId);
      Assert.fail();
    } catch (ProfileConflictException e) {
      // expected
    }

    // enable should work this time
    profileService.enableProfile(profileId);
    Assert.assertEquals(ProfileStatus.ENABLED, profileService.getProfile(profileId).getStatus());

    // delete the second profile should fail since it is enabled
    try {
      profileService.deleteProfile(profileId2);
      Assert.fail();
    } catch (ProfileConflictException e) {
      // expected
    }
    profileService.disableProfile(profileId2);
    profileService.deleteProfile(profileId2);
    Assert.assertEquals(ImmutableList.of(expected), profileService.getProfiles(NamespaceId.DEFAULT, false));

    // add one and delete all profiles
    profileService.saveProfile(profileId2, profile2);
    profileService.disableProfile(profileId2);
    profileService.deleteAllProfiles(NamespaceId.DEFAULT);
    Assert.assertEquals(Collections.EMPTY_LIST, profileService.getProfiles(NamespaceId.DEFAULT, false));

    // try to enable and disable an non-existing profile should throw NotFoundException
    try {
      profileService.enableProfile(profileId);
      Assert.fail();
    } catch (NotFoundException e) {
      // expected
    }

    try {
      profileService.disableProfile(profileId);
      Assert.fail();
    } catch (NotFoundException e) {
      // expected
    }
  }
}
