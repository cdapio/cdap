/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.config;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProfileConflictException;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.profile.Profile;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tests for PreferencesService.
 */
public class PreferencesServiceTest extends AppFabricTestBase {

  // Testing PreferencesStore
  @Test
  public void testCleanSlate() throws Exception {
    Map<String, String> emptyMap = ImmutableMap.of();
    PreferencesService store = getInjector().getInstance(PreferencesService.class);
    Assert.assertEquals(emptyMap, store.getProperties());
    Assert.assertEquals(emptyMap, store.getProperties(new NamespaceId("somenamespace")));
    Assert.assertEquals(emptyMap, store.getProperties(NamespaceId.DEFAULT));
    Assert.assertEquals(emptyMap, store.getResolvedProperties());
    Assert.assertEquals(emptyMap, store.getResolvedProperties(new ProgramId("a", "b", ProgramType.WORKFLOW, "d")));
    // should not throw any exception if try to delete properties without storing anything
    store.deleteProperties();
    store.deleteProperties(NamespaceId.DEFAULT);
    store.deleteProperties(new ProgramId("a", "x", ProgramType.WORKFLOW, "z"));

    // Get instance level PreferencesDetail. None has been set, so seqId should be 0 and it should monotonically
    // increasing as preferences are mutated (i.e. set or deleted) for the entity.
    PreferencesDetail detail = store.getPreferences();
    Assert.assertEquals(emptyMap, detail.getProperties());
    Assert.assertEquals(0, detail.getSeqId());
  }

  @Test
  public void testBasicProperties() throws Exception {
    Map<String, String> propMap = Maps.newHashMap();
    propMap.put("key", "instance");
    PreferencesService store = getInjector().getInstance(PreferencesService.class);
    store.setProperties(propMap);
    Assert.assertEquals(propMap, store.getProperties());
    Assert.assertEquals(propMap, store.getResolvedProperties(new ProgramId("a", "b", ProgramType.WORKFLOW, "d")));
    Assert.assertEquals(propMap, store.getResolvedProperties(new NamespaceId("myspace")));
    Assert.assertEquals(ImmutableMap.<String, String>of(), store.getProperties(new NamespaceId("myspace")));
    store.deleteProperties();
    propMap.clear();
    Assert.assertEquals(propMap, store.getProperties());
    Assert.assertEquals(propMap, store.getResolvedProperties(new ProgramId("a", "b", ProgramType.WORKFLOW, "d")));
    Assert.assertEquals(propMap, store.getResolvedProperties(new NamespaceId("myspace")));
  }

  @Test
  public void testMultiLevelProperties() throws Exception {
    Map<String, String> propMap = Maps.newHashMap();
    propMap.put("key", "namespace");
    PreferencesService store = getInjector().getInstance(PreferencesService.class);
    store.setProperties(new NamespaceId("myspace"), propMap);
    propMap.put("key", "application");
    store.setProperties(new ApplicationId("myspace", "app"), propMap);
    Assert.assertEquals(propMap, store.getProperties(new ApplicationId("myspace", "app")));
    Assert.assertEquals("namespace", store.getProperties(new NamespaceId("myspace")).get("key"));
    Assert.assertTrue(store.getProperties(new ApplicationId("myspace", "notmyapp")).isEmpty());
    Assert.assertEquals("namespace", store.getResolvedProperties(new ApplicationId("myspace", "notmyapp")).get("key"));
    Assert.assertTrue(store.getProperties(new NamespaceId("notmyspace")).isEmpty());
    store.deleteProperties(new NamespaceId("myspace"));
    Assert.assertTrue(store.getProperties(new NamespaceId("myspace")).isEmpty());
    Assert.assertTrue(store.getResolvedProperties(new ApplicationId("myspace", "notmyapp")).isEmpty());
    Assert.assertEquals(propMap, store.getProperties(new ApplicationId("myspace", "app")));
    store.deleteProperties(new ApplicationId("myspace", "app"));
    Assert.assertTrue(store.getProperties(new ApplicationId("myspace", "app")).isEmpty());
    propMap.put("key", "program");
    store.setProperties(new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog"), propMap);
    Assert.assertEquals(propMap, store.getProperties(new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog")));
    store.setProperties(ImmutableMap.of("key", "instance"));
    Assert.assertEquals(propMap, store.getProperties(new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog")));
    store.deleteProperties(new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog"));
    Assert.assertTrue(store.getProperties(new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog")).isEmpty());
    Assert.assertEquals("instance", store.getResolvedProperties(
      new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog")).get("key"));
    store.deleteProperties();
    Assert.assertEquals(ImmutableMap.<String, String>of(), store.getProperties(
      new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog")));
  }

  @Test
  public void testAddProfileInProperties() throws Exception {
    PreferencesService prefStore = getInjector().getInstance(PreferencesService.class);
    ProfileService profileStore = getInjector().getInstance(ProfileService.class);

    // put a profile unrelated property should not affect the write
    Map<String, String> expected = new HashMap<>();
    expected.put("unRelatedKey", "unRelatedValue");
    prefStore.setProperties(new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog"), expected);
    Assert.assertEquals(expected, prefStore.getProperties(
      new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog")));

    // put something related to profile
    Map<String, String> profileMap = new HashMap<>();
    profileMap.put(SystemArguments.PROFILE_NAME, "userProfile");

    // this set call should fail since the profile does not exist
    try {
      prefStore.setProperties(new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog"), profileMap);
      Assert.fail();
    } catch (NotFoundException e) {
      // expected
    }
    // the pref store should remain unchanged
    Assert.assertEquals(expected, prefStore.getProperties(
      new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog")));

    // add the profile and disable it
    ProfileId profileId = new ProfileId("myspace", "userProfile");
    profileStore.saveProfile(profileId, Profile.NATIVE);
    profileStore.disableProfile(profileId);

    // this set call should fail since the profile is disabled
    try {
      prefStore.setProperties(new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog"), profileMap);
      Assert.fail();
    } catch (ProfileConflictException e) {
      // expected
    }
    // the pref store should remain unchanged
    Assert.assertEquals(expected, prefStore.getProperties(
      new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog")));

    // enable the profile
    profileStore.enableProfile(profileId);
    expected = profileMap;
    prefStore.setProperties(new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog"), profileMap);
    Map<String, String> properties = prefStore.getProperties(
      new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog"));
    Assert.assertEquals(expected, properties);

    prefStore.deleteProperties(new ProgramId("myspace", "app", ProgramType.WORKFLOW, "prog"));
    profileStore.disableProfile(profileId);
    profileStore.deleteProfile(profileId);
  }

  @Test
  public void testAddUserProfileToPreferencesInstanceLevel() throws Exception {
    PreferencesService prefStore = getInjector().getInstance(PreferencesService.class);

    // use profile in USER scope at instance level should fail
    try {
      prefStore.setProperties(Collections.singletonMap(SystemArguments.PROFILE_NAME, "userProfile"));
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }

    try {
      prefStore.setProperties(Collections.singletonMap(SystemArguments.PROFILE_NAME, "USER:userProfile"));
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }
  }

  @Test
  public void testProfileAssignment() throws Exception {
    PreferencesService preferencesService = getInjector().getInstance(PreferencesService.class);
    ProfileService profileService = getInjector().getInstance(ProfileService.class);
    ProfileId myProfile = NamespaceId.DEFAULT.profile("myProfile");
    profileService.saveProfile(myProfile, Profile.NATIVE);

    // add properties with profile information
    Map<String, String> prop = new HashMap<>();
    prop.put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());
    ApplicationId myApp = NamespaceId.DEFAULT.app("myApp");
    ProgramId myProgram = myApp.workflow("myProgram");
    preferencesService.setProperties(prop);
    preferencesService.setProperties(NamespaceId.DEFAULT, prop);
    preferencesService.setProperties(myApp, prop);
    preferencesService.setProperties(myProgram, prop);

    // the assignment should be there for these entities
    Set<EntityId> expected = new HashSet<>();
    expected.add(new InstanceId(""));
    expected.add(NamespaceId.DEFAULT);
    expected.add(myApp);
    expected.add(myProgram);
    Assert.assertEquals(expected, profileService.getProfileAssignments(ProfileId.NATIVE));

    // setting an empty property is actually deleting the assignment
    prop.clear();
    preferencesService.setProperties(myApp, prop);
    expected.remove(myApp);
    Assert.assertEquals(expected, profileService.getProfileAssignments(ProfileId.NATIVE));

    // set my program to use a different profile, should update both profiles
    prop.put(SystemArguments.PROFILE_NAME, myProfile.getScopedName());
    preferencesService.setProperties(myProgram, prop);
    expected.remove(myProgram);
    Assert.assertEquals(expected, profileService.getProfileAssignments(ProfileId.NATIVE));
    Assert.assertEquals(Collections.singleton(myProgram), profileService.getProfileAssignments(myProfile));

    // delete all preferences
    preferencesService.deleteProperties();
    preferencesService.deleteProperties(NamespaceId.DEFAULT);
    preferencesService.deleteProperties(myApp);
    preferencesService.deleteProperties(myProgram);
    Assert.assertEquals(Collections.emptySet(), profileService.getProfileAssignments(ProfileId.NATIVE));
    Assert.assertEquals(Collections.emptySet(), profileService.getProfileAssignments(myProfile));

    profileService.disableProfile(myProfile);
    profileService.deleteProfile(myProfile);
  }
}
