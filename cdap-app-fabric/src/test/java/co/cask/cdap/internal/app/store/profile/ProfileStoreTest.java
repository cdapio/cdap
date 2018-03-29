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

package co.cask.cdap.internal.app.store.profile;

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.profile.ProvisionerInfo;
import co.cask.cdap.proto.profile.ProvisionerPropertyValue;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Unit test for profile store
 */
public class ProfileStoreTest {
  private static final List<ProvisionerPropertyValue> PROPERTY_SUMMARIES =
    ImmutableList.<ProvisionerPropertyValue>builder()
      .add(new ProvisionerPropertyValue("1st property", "1st value", false))
      .add(new ProvisionerPropertyValue("2nd property", "2nd value", true))
      .add(new ProvisionerPropertyValue("3rd property", "3rd value", false))
      .build();

  private static ProfileStore profileStore;

  @BeforeClass
  public static void setup() throws Exception {
    profileStore = AppFabricTestHelper.getInjector().getInstance(ProfileStore.class);
  }

  @Test
  public void testProfileStore() throws Exception {
    // get non-existing profile
    try {
      profileStore.getProfile(NamespaceId.DEFAULT.profile("nonExisting"));
      Assert.fail();
    } catch (NotFoundException e) {
      // expected
    }

    // delete non-existing profile
    try {
      profileStore.delete(NamespaceId.DEFAULT.profile("nonExisting"));
      Assert.fail();
    } catch (NotFoundException e) {
      // expected
    }

    ProfileId profileId = NamespaceId.DEFAULT.profile("MyProfile");
    Profile expected = new Profile("MyProfile", "my profile for testing",
                                   new ProvisionerInfo("defaultProvisioner", PROPERTY_SUMMARIES));
    // add a profile
    profileStore.add(profileId, expected);

    // get the profile
    Assert.assertEquals(expected, profileStore.getProfile(profileId));

    // add a profile which already exists
    try {
      profileStore.add(profileId, new Profile("MyProfile", "my another profile",
                                              new ProvisionerInfo("defaultProvisioner", PROPERTY_SUMMARIES)));
      Assert.fail();  
    } catch (AlreadyExistsException e) {
      // expected
    }

    // add another profile to default namespace
    ProfileId profileId2 = NamespaceId.DEFAULT.profile("MyProfile2");
    Profile profile2 = new Profile("MyProfile2", "my 2nd profile for testing",
                                   new ProvisionerInfo("anotherProvisioner", PROPERTY_SUMMARIES));
    profileStore.add(profileId2, profile2);

    // get all profiles
    List<Profile> profiles = ImmutableList.of(expected, profile2);
    Assert.assertEquals(profiles, profileStore.getProfiles(NamespaceId.DEFAULT));

    // delete the second profile
    profileStore.delete(profileId2);
    Assert.assertEquals(ImmutableList.of(expected), profileStore.getProfiles(NamespaceId.DEFAULT));

    // add one and delete all profiles
    profileStore.add(profileId2, profile2);
    profileStore.deleteAll(NamespaceId.DEFAULT);
    Assert.assertEquals(Collections.EMPTY_LIST, profileStore.getProfiles(NamespaceId.DEFAULT));
  }
}
