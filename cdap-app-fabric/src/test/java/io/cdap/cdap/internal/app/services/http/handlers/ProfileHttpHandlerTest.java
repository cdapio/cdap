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

package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.provision.MockProvisioner;
import io.cdap.cdap.internal.provision.MockProvisionerWithCpus;
import io.cdap.cdap.proto.EntityScope;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.proto.provisioner.ProvisionerDetail;
import io.cdap.cdap.proto.provisioner.ProvisionerInfo;
import io.cdap.cdap.proto.provisioner.ProvisionerPropertyValue;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.runtime.spi.profile.ProfileStatus;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import io.cdap.cdap.security.spi.authorization.PermissionManager;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for profile http handler
 */
public class ProfileHttpHandlerTest extends AppFabricTestBase {
  private static final Set<ProvisionerPropertyValue> PROPERTY_SUMMARIES =
    ImmutableSet.<ProvisionerPropertyValue>builder()
      .add(new ProvisionerPropertyValue("1st property", "1st value", false))
      .add(new ProvisionerPropertyValue("2nd property", "2nd value", true))
      .add(new ProvisionerPropertyValue("3rd property", "3rd value", false))
      .build();

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  public static final String READ_ONLY_SYSTEM_USER_NAME = "readOnlySystem";
  public static final String READ_ONLY_USER_NAME = "readOnly";
  public static final String READ_WRITE_USER_NAME = "readWrite";
  public static final String NO_ACCESS_USER_NAME = "noAccess";
  public static final Principal READ_ONLY_USER = new Principal(READ_ONLY_USER_NAME, Principal.PrincipalType.USER);
  public static final Principal READ_ONLY_SYSTEM_USER =
    new Principal(READ_ONLY_SYSTEM_USER_NAME, Principal.PrincipalType.USER);
  public static final Principal READ_WRITE_USER = new Principal(READ_WRITE_USER_NAME, Principal.PrincipalType.USER);
  public static final Principal CREATE_PROFILE_USER = new Principal("createProfileUser",
                                                                    Principal.PrincipalType.USER);
  public static final Principal UPDATE_PROFILE_USER = new Principal("updateProfileUser",
                                                                    Principal.PrincipalType.USER);
  public static final Principal DELETE_PROFILE_USER = new Principal("deleteProfileUser",
                                                                    Principal.PrincipalType.USER);
  public static final String PERMISSIONS_TEST_PROFILE = "permissionsTestProfile";

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration cConf = AppFabricTestHelper.enableAuthorization(
        createBasicCconf(), TEMPORARY_FOLDER);
    initializeAndStartServices(cConf);
    PermissionManager permissionManager = getInjector().getInstance(PermissionManager.class);
    permissionManager.grant(Authorizable.fromEntityId(NamespaceId.SYSTEM, EntityType.PROFILE), READ_WRITE_USER,
                            ImmutableSet.of(StandardPermission.LIST));
    permissionManager.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT, EntityType.PROFILE), READ_WRITE_USER,
                            ImmutableSet.of(StandardPermission.LIST));
    permissionManager.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT), READ_WRITE_USER,
                            EnumSet.allOf(StandardPermission.class));
    permissionManager.grant(Authorizable.fromString("profile:system.p1"), READ_WRITE_USER,
                            EnumSet.allOf(StandardPermission.class));
    permissionManager.grant(Authorizable.fromString("profile:default.MyProfile"), READ_WRITE_USER,
                            EnumSet.allOf(StandardPermission.class));
    permissionManager.grant(Authorizable.fromString("profile:system." + Profile.NATIVE_NAME), READ_WRITE_USER,
                            EnumSet.allOf(StandardPermission.class));


    permissionManager.grant(Authorizable.fromEntityId(NamespaceId.SYSTEM, EntityType.PROFILE), READ_ONLY_SYSTEM_USER,
                            ImmutableSet.of(StandardPermission.LIST));
    permissionManager.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT, EntityType.PROFILE), READ_ONLY_SYSTEM_USER,
                            ImmutableSet.of(StandardPermission.LIST));
    permissionManager.grant(Authorizable.fromString("profile:system." + Profile.NATIVE_NAME), READ_ONLY_SYSTEM_USER,
                            EnumSet.of(StandardPermission.GET));

    permissionManager.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT, EntityType.PROFILE), READ_ONLY_USER,
                            ImmutableSet.of(StandardPermission.LIST));
    permissionManager.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT.profile("p1")), READ_ONLY_USER,
                            EnumSet.of(StandardPermission.GET));

    permissionManager.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT.profile(PERMISSIONS_TEST_PROFILE)),
                            CREATE_PROFILE_USER, EnumSet.of(StandardPermission.CREATE));
    permissionManager.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT.profile(PERMISSIONS_TEST_PROFILE)),
                            UPDATE_PROFILE_USER, EnumSet.of(StandardPermission.UPDATE));
    permissionManager.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT.profile(PERMISSIONS_TEST_PROFILE)),
                            DELETE_PROFILE_USER, EnumSet.of(StandardPermission.DELETE));
    permissionManager.grant(Authorizable.fromEntityId(NamespaceId.SYSTEM.profile(PERMISSIONS_TEST_PROFILE)),
                            CREATE_PROFILE_USER, EnumSet.of(StandardPermission.CREATE));
    permissionManager.grant(Authorizable.fromEntityId(NamespaceId.SYSTEM.profile(PERMISSIONS_TEST_PROFILE)),
                            UPDATE_PROFILE_USER, EnumSet.of(StandardPermission.UPDATE));
    permissionManager.grant(Authorizable.fromEntityId(NamespaceId.SYSTEM.profile(PERMISSIONS_TEST_PROFILE)),
                            DELETE_PROFILE_USER, EnumSet.of(StandardPermission.DELETE));
  }

  @Test
  public void testSystemProfiles() throws Exception {
    Assert.assertEquals(Collections.singletonList(Profile.NATIVE), listSystemProfiles(HttpURLConnection.HTTP_OK));

    Profile p1 = new Profile("p1", "label", "desc", EntityScope.SYSTEM,
                             new ProvisionerInfo(MockProvisioner.NAME, PROPERTY_SUMMARIES));
    putSystemProfile(p1.getName(), p1, HttpURLConnection.HTTP_OK);
    Optional<Profile> p1Optional = getSystemProfile(p1.getName(), HttpURLConnection.HTTP_OK);
    Assert.assertTrue(p1Optional.isPresent());
    Assert.assertEquals(p1, p1Optional.get());

    // check list contains both native and p1
    Set<Profile> expected = new HashSet<>();
    expected.add(Profile.NATIVE);
    expected.add(p1);
    Set<Profile> actual = new HashSet<>(listSystemProfiles(HttpURLConnection.HTTP_OK));
    Assert.assertEquals(expected, actual);
    // check that they're both visible to namespaces
    Assert.assertEquals(expected, new HashSet<>(listProfiles(NamespaceId.DEFAULT, true, HttpURLConnection.HTTP_OK)));

    // check we can add a profile with the same name in a namespace
    Profile p2 = new Profile(p1.getName(), p1.getLabel(), p1.getDescription(), EntityScope.USER,
                             p1.getProvisioner());
    ProfileId p2Id = NamespaceId.DEFAULT.profile(p2.getName());
    putProfile(p2Id, p2, HttpURLConnection.HTTP_OK);
    // check that all are visible to the namespace
    expected.add(p2);
    Assert.assertEquals(expected, new HashSet<>(listProfiles(NamespaceId.DEFAULT, true, HttpURLConnection.HTTP_OK)));
    // check that namespaced profile is not visible in system list
    expected.remove(p2);
    Assert.assertEquals(expected, new HashSet<>(listSystemProfiles(HttpURLConnection.HTTP_OK)));

    disableProfile(p2Id, HttpURLConnection.HTTP_OK);
    deleteProfile(p2Id, HttpURLConnection.HTTP_OK);
    disableSystemProfile(p1.getName(), HttpURLConnection.HTTP_OK);
    deleteSystemProfile(p1.getName(), HttpURLConnection.HTTP_OK);

    doAs(READ_WRITE_USER_NAME, () -> {
      listSystemProfiles(HttpURLConnection.HTTP_OK);
      listProfiles(NamespaceId.DEFAULT, true, HttpURLConnection.HTTP_OK);
      putSystemProfile(p1.getName(), p1, HttpURLConnection.HTTP_OK);
      getSystemProfile(Profile.NATIVE.getName(), HttpURLConnection.HTTP_OK);
      disableSystemProfile(p1.getName(), HttpURLConnection.HTTP_OK);
      enableSystemProfile(p1.getName(), HttpURLConnection.HTTP_OK);
      disableSystemProfile(p1.getName(), HttpURLConnection.HTTP_OK);
      deleteSystemProfile(p1.getName(), HttpURLConnection.HTTP_OK);
    });
    doAs(READ_ONLY_SYSTEM_USER_NAME, () -> {
      listSystemProfiles(HttpURLConnection.HTTP_OK);
      listProfiles(NamespaceId.DEFAULT, true, HttpURLConnection.HTTP_OK);
      putSystemProfile(p1.getName(), p1, HttpURLConnection.HTTP_FORBIDDEN);
      getSystemProfile(Profile.NATIVE.getName(), HttpURLConnection.HTTP_OK);
      disableSystemProfile(p1.getName(), HttpURLConnection.HTTP_FORBIDDEN);
      enableSystemProfile(p1.getName(), HttpURLConnection.HTTP_FORBIDDEN);
      deleteSystemProfile(p1.getName(), HttpURLConnection.HTTP_FORBIDDEN);
    });
    doAs(READ_ONLY_USER_NAME, () -> {
      listSystemProfiles(HttpURLConnection.HTTP_FORBIDDEN);
      listProfiles(NamespaceId.DEFAULT, true, HttpURLConnection.HTTP_FORBIDDEN);
      listProfiles(NamespaceId.DEFAULT, false, HttpURLConnection.HTTP_OK);
      getSystemProfile(Profile.NATIVE.getName(), HttpURLConnection.HTTP_FORBIDDEN);
      putSystemProfile(p1.getName(), p1, HttpURLConnection.HTTP_FORBIDDEN);
      disableSystemProfile(p1.getName(), HttpURLConnection.HTTP_FORBIDDEN);
      enableSystemProfile(p1.getName(), HttpURLConnection.HTTP_FORBIDDEN);
      deleteSystemProfile(p1.getName(), HttpURLConnection.HTTP_FORBIDDEN);
    });
  }

  @Test
  public void testSystemNamespaceProfilesNotAllowed() throws Exception {
    listProfiles(NamespaceId.SYSTEM, false, HttpURLConnection.HTTP_BAD_METHOD);
    getProfile(NamespaceId.SYSTEM.profile("abc"), HttpURLConnection.HTTP_BAD_METHOD);
    disableProfile(NamespaceId.SYSTEM.profile("abc"), HttpURLConnection.HTTP_BAD_METHOD);
    enableProfile(NamespaceId.SYSTEM.profile("abc"), HttpURLConnection.HTTP_BAD_METHOD);

    Profile profile = new Profile("abc", "label", "desc", new ProvisionerInfo("xyz", Collections.emptyList()));
    putProfile(NamespaceId.SYSTEM.profile("abc"), profile, HttpURLConnection.HTTP_BAD_METHOD);
  }

  @Test
  public void testListAndGetProfiles() throws Exception {
    // no profile should be there in default namespace
    List<Profile> profiles = listProfiles(NamespaceId.DEFAULT, false, HttpURLConnection.HTTP_OK);
    Assert.assertEquals(Collections.emptyList(), profiles);

    // try to list all profiles including system namespace before putting a new one, there should only exist a default
    // profile
    profiles = listProfiles(NamespaceId.DEFAULT, true, HttpURLConnection.HTTP_OK);
    Assert.assertEquals(Collections.singletonList(Profile.NATIVE), profiles);

    // test get single profile endpoint
    ProfileId profileId = NamespaceId.DEFAULT.profile("p1");
    Profile expected = new Profile("p1", "label", "my profile for testing",
                                   new ProvisionerInfo(MockProvisioner.NAME, PROPERTY_SUMMARIES));
    putProfile(profileId, expected, HttpURLConnection.HTTP_OK);
    Profile actual = getProfile(profileId, HttpURLConnection.HTTP_OK).get();
    Assert.assertEquals(expected, actual);

    // get a nonexisting profile should get a not found code
    getProfile(NamespaceId.DEFAULT.profile("nonExisting"), HttpURLConnection.HTTP_NOT_FOUND);
    doAs(READ_ONLY_USER_NAME, () -> {
      listProfiles(NamespaceId.DEFAULT, false, HttpURLConnection.HTTP_OK);
      getProfile(profileId, HttpURLConnection.HTTP_OK);
    });
    doAs(NO_ACCESS_USER_NAME, () -> {
      listProfiles(NamespaceId.DEFAULT, false, HttpURLConnection.HTTP_FORBIDDEN);
      getProfile(profileId, HttpURLConnection.HTTP_FORBIDDEN);
    });
  }

  @Test
  public void testPutAndDeleteProfiles() throws Exception {
    Profile invalidProfile = new Profile("MyProfile", "label", "my profile for testing",
                                         new ProvisionerInfo("nonExisting", PROPERTY_SUMMARIES));
    // adding a profile with non-existing provisioner should get a 400
    putProfile(NamespaceId.DEFAULT.profile(invalidProfile.getName()), invalidProfile,
               HttpURLConnection.HTTP_BAD_REQUEST);

    // put a profile with the mock provisioner
    Profile expected = new Profile("MyProfile", "label", "my profile for testing",
                                   new ProvisionerInfo(MockProvisioner.NAME, PROPERTY_SUMMARIES));
    ProfileId expectedProfileId = NamespaceId.DEFAULT.profile(expected.getName());
    putProfile(expectedProfileId, expected, HttpURLConnection.HTTP_OK);

    // get the profile
    Profile actual = getProfile(expectedProfileId, HttpURLConnection.HTTP_OK).get();
    Assert.assertEquals(expected, actual);

    // list all profiles, should get 2 profiles
    List<Profile> profiles = listProfiles(NamespaceId.DEFAULT, true, HttpURLConnection.HTTP_OK);
    Set<Profile> expectedList = ImmutableSet.of(Profile.NATIVE, expected);
    Assert.assertEquals(expectedList.size(), profiles.size());
    Assert.assertEquals(expectedList, new HashSet<>(profiles));

    // adding the same profile should still succeed
    putProfile(expectedProfileId, expected, HttpURLConnection.HTTP_OK);

    // get non-existing profile should get a 404
    deleteProfile(NamespaceId.DEFAULT.profile("nonExisting"), HttpURLConnection.HTTP_NOT_FOUND);

    // delete the profile should fail first time since it is by default enabled
    deleteProfile(expectedProfileId, HttpURLConnection.HTTP_CONFLICT);

    // disable the profile then delete should work
    disableProfile(expectedProfileId, HttpURLConnection.HTTP_OK);
    deleteProfile(expectedProfileId, HttpURLConnection.HTTP_OK);

    Assert.assertEquals(Collections.emptyList(), listProfiles(NamespaceId.DEFAULT, false, HttpURLConnection.HTTP_OK));

    // if given some unrelated json, it should return a 400 instead of 500
    ProvisionerSpecification spec = new MockProvisioner().getSpec();
    ProvisionerDetail test = new ProvisionerDetail(spec.getName(), spec.getLabel(), spec.getDescription(),
                                                   new ArrayList<>(), null, null, false);
    putProfile(NamespaceId.DEFAULT.profile(test.getName()), test, HttpURLConnection.HTTP_BAD_REQUEST);

    doAs(READ_ONLY_USER_NAME, () -> {
      putProfile(expectedProfileId, expected, HttpURLConnection.HTTP_FORBIDDEN);
      disableProfile(expectedProfileId, HttpURLConnection.HTTP_FORBIDDEN);
      enableProfile(expectedProfileId, HttpURLConnection.HTTP_FORBIDDEN);
      deleteProfile(expectedProfileId, HttpURLConnection.HTTP_FORBIDDEN);
    });
    doAs(READ_WRITE_USER_NAME, () -> {
      putProfile(expectedProfileId, expected, HttpURLConnection.HTTP_OK);
      disableProfile(expectedProfileId, HttpURLConnection.HTTP_OK);
      enableProfile(expectedProfileId, HttpURLConnection.HTTP_OK);
      disableProfile(expectedProfileId, HttpURLConnection.HTTP_OK);
      deleteProfile(expectedProfileId, HttpURLConnection.HTTP_OK);
    });
  }

  @Test
  public void testEnableDisableProfile() throws Exception {
    Profile expected = new Profile("MyProfile", "label", "my profile for testing",
      new ProvisionerInfo(MockProvisioner.NAME, PROPERTY_SUMMARIES));
    ProfileId profileId = NamespaceId.DEFAULT.profile(expected.getName());

    // enable and disable a non-existing profile should give a 404
    enableProfile(profileId, HttpURLConnection.HTTP_NOT_FOUND);
    disableProfile(profileId, HttpURLConnection.HTTP_NOT_FOUND);

    // put the profile
    putProfile(profileId, expected, HttpURLConnection.HTTP_OK);

    // by default the status should be enabled
    Assert.assertEquals(ProfileStatus.ENABLED, getProfileStatus(profileId, HttpURLConnection.HTTP_OK).get());

    // enable it again should give a 409
    enableProfile(profileId, HttpURLConnection.HTTP_CONFLICT);

    // disable should work
    disableProfile(profileId, HttpURLConnection.HTTP_OK);
    Assert.assertEquals(ProfileStatus.DISABLED, getProfileStatus(profileId, HttpURLConnection.HTTP_OK).get());

    // disable again should give a 409
    disableProfile(profileId, HttpURLConnection.HTTP_CONFLICT);

    // enable should work
    enableProfile(profileId, HttpURLConnection.HTTP_OK);
    Assert.assertEquals(ProfileStatus.ENABLED, getProfileStatus(profileId, HttpURLConnection.HTTP_OK).get());

    // now delete should not work since we have the profile enabled
    deleteProfile(profileId, HttpURLConnection.HTTP_CONFLICT);

    // disable and delete
    disableProfile(profileId, HttpURLConnection.HTTP_OK);
    deleteProfile(profileId, HttpURLConnection.HTTP_OK);
  }

  @Test
  public void testNullProvisionerProperty() throws Exception {
    // provide a profile with null provsioner property, it should still succeed
    List<ProvisionerPropertyValue> listWithNull = new ArrayList<>();
    listWithNull.add(null);
    Profile profile = new Profile("ProfileWithNull", "label", "should succeed",
      new ProvisionerInfo(MockProvisioner.NAME, listWithNull));
    putProfile(NamespaceId.DEFAULT.profile(profile.getName()), profile, HttpURLConnection.HTTP_OK);

    // Get the profile, it should not contain the null value, the property should be an empty list
    Profile actual = getProfile(NamespaceId.DEFAULT.profile(profile.getName()), HttpURLConnection.HTTP_OK).get();
    Assert.assertNotNull(actual);
    Assert.assertEquals(Collections.EMPTY_SET, actual.getProvisioner().getProperties());

    disableProfile(NamespaceId.DEFAULT.profile(profile.getName()), HttpURLConnection.HTTP_OK);
    deleteProfile(NamespaceId.DEFAULT.profile(profile.getName()), HttpURLConnection.HTTP_OK);

    // provide a profile with mixed properties with null, it should still succeed
    List<ProvisionerPropertyValue> listMixed = new ArrayList<>(PROPERTY_SUMMARIES);
    listMixed.addAll(listWithNull);
    profile = new Profile("ProfileMixed", "label", "should succeed",
      new ProvisionerInfo(MockProvisioner.NAME, listMixed));
    putProfile(NamespaceId.DEFAULT.profile(profile.getName()), profile, HttpURLConnection.HTTP_OK);

    // Get the profile, it should not contain the null value, the property should be all non-null properties in the list
    actual = getProfile(NamespaceId.DEFAULT.profile(profile.getName()), HttpURLConnection.HTTP_OK).get();
    Assert.assertNotNull(actual);
    Assert.assertEquals(PROPERTY_SUMMARIES, actual.getProvisioner().getProperties());
    disableProfile(NamespaceId.DEFAULT.profile(profile.getName()), HttpURLConnection.HTTP_OK);
    deleteProfile(NamespaceId.DEFAULT.profile(profile.getName()), HttpURLConnection.HTTP_OK);
  }

  @Test
  public void testNativeProfileImmutable() throws Exception {
    // verify native profile exists
    Assert.assertEquals(Profile.NATIVE, getSystemProfile(ProfileId.NATIVE.getProfile(),
                                                         HttpURLConnection.HTTP_OK).get());
    // disable, update, or delete should throw a 405
    disableSystemProfile(ProfileId.NATIVE.getProfile(), HttpURLConnection.HTTP_BAD_METHOD);
    putSystemProfile(ProfileId.NATIVE.getProfile(), Profile.NATIVE, HttpURLConnection.HTTP_BAD_METHOD);
    deleteSystemProfile(ProfileId.NATIVE.getProfile(), HttpURLConnection.HTTP_BAD_METHOD);
  }

  @Test
  public void testPutPermissionChecks() throws Exception {
    Profile profile = new Profile(PERMISSIONS_TEST_PROFILE, "label", "default permissions testing profile",
                                         new ProvisionerInfo(MockProvisioner.NAME, Collections.emptyList()));
    ProfileId profileId = NamespaceId.DEFAULT.profile(PERMISSIONS_TEST_PROFILE);
    // Verify the UPDATE user does not have permissions to create a profile
    doAs(UPDATE_PROFILE_USER.getName(), () -> {
      putProfile(profileId, profile, HttpURLConnection.HTTP_FORBIDDEN);
      putSystemProfile(profile.getName(), profile, HttpURLConnection.HTTP_FORBIDDEN);
    });

    // Verify that the CREATE user can create both profiles
    doAs(CREATE_PROFILE_USER.getName(), () -> {
      putProfile(profileId, profile, HttpURLConnection.HTTP_OK);
      putSystemProfile(profile.getName(), profile, HttpURLConnection.HTTP_OK);
    });

    // Verify that the UPDATE user can modify the profiles after they have been created
    doAs(UPDATE_PROFILE_USER.getName(), () -> {
      putProfile(profileId, profile, HttpURLConnection.HTTP_OK);
      putSystemProfile(profile.getName(), profile, HttpURLConnection.HTTP_OK);
      // Disable the profiles in preparation for deletion.
      disableProfile(profileId, HttpURLConnection.HTTP_OK);
      disableSystemProfile(profile.getName(), HttpURLConnection.HTTP_OK);
      // Verify deletion failure
      deleteProfile(profileId, HttpURLConnection.HTTP_FORBIDDEN);
      deleteSystemProfile(profile.getName(), HttpURLConnection.HTTP_FORBIDDEN);
    });

    // Verify that the CREATE user cannot update profiles after they have been created
    doAs(CREATE_PROFILE_USER.getName(), () -> {
      putProfile(profileId, profile, HttpURLConnection.HTTP_FORBIDDEN);
      putSystemProfile(profile.getName(), profile, HttpURLConnection.HTTP_FORBIDDEN);
    });

    // Delete profiles
    doAs(DELETE_PROFILE_USER.getName(), () -> {
      deleteProfile(profileId, HttpURLConnection.HTTP_OK);
      deleteSystemProfile(profile.getName(), HttpURLConnection.HTTP_OK);
    });
  }

  @Test
  public void testPutAndGetProfileWithTotalCPUSImplementedProvisioner() throws Exception {
    // put a profile with the mock provisioner with total cpus method implemented
    Profile expected = new Profile("MyProfile", "label", "my profile for testing",
                                   new ProvisionerInfo(MockProvisionerWithCpus.NAME, PROPERTY_SUMMARIES));
    ProfileId expectedProfileId = NamespaceId.DEFAULT.profile(expected.getName());
    putProfile(expectedProfileId, expected, HttpURLConnection.HTTP_OK);

    // get the profile
    Profile actual = getProfile(expectedProfileId, HttpURLConnection.HTTP_OK).get();

    expected.getProvisioner().setTotalProcessingCpusLabel(MockProvisionerWithCpus.TEST_LABEL);
    Assert.assertEquals(expected, actual);

    disableProfile(expectedProfileId, HttpURLConnection.HTTP_OK);
    deleteProfile(expectedProfileId, HttpURLConnection.HTTP_OK);
  }

}
