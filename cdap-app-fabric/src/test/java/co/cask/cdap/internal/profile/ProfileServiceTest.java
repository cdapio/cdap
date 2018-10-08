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

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.MethodNotAllowedException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProfileConflictException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.provisioner.ProvisionerInfo;
import co.cask.cdap.proto.provisioner.ProvisionerPropertyValue;
import co.cask.cdap.runtime.spi.profile.ProfileStatus;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.twill.api.RunId;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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

  private static Injector injector;
  private static ProfileService profileService;

  @BeforeClass
  public static void setup() {
    injector = AppFabricTestHelper.getInjector();
    profileService = injector.getInstance(ProfileService.class);
  }

  @After
  public void afterTest() throws Exception {
    profileService.clear();
  }

  @Test
  public void testProfileOverrides() throws Exception {
    List<ProvisionerPropertyValue> provisionerProperties = new ArrayList<>();
    provisionerProperties.add(new ProvisionerPropertyValue("editable1", "val", true));
    provisionerProperties.add(new ProvisionerPropertyValue("editable2", "val", true));
    provisionerProperties.add(new ProvisionerPropertyValue("final", "finalval", false));

    ProvisionerInfo provisionerInfo = new ProvisionerInfo("provisioner", provisionerProperties);
    Profile profile = new Profile("name", "label", "desc", provisionerInfo);
    ProfileId profileId = NamespaceId.DEFAULT.profile("p");
    profileService.saveProfile(profileId, profile);

    try {
      Map<String, String> args = new HashMap<>();
      args.put("editable1", "newval");
      args.put("final", "shouldnotwork");
      args.put("newarg", "val");

      // resolved properties should include all the stored properties,
      // with 'final' not overridden and 'editable1' overridden.
      List<ProvisionerPropertyValue> expectedProperties = new ArrayList<>();
      expectedProperties.add(new ProvisionerPropertyValue("editable1", "newval", true));
      expectedProperties.add(new ProvisionerPropertyValue("editable2", "val", true));
      expectedProperties.add(new ProvisionerPropertyValue("final", "finalval", false));
      expectedProperties.add(new ProvisionerPropertyValue("newarg", "val", true));
      provisionerInfo = new ProvisionerInfo(provisionerInfo.getName(), expectedProperties);
      Profile expected = new Profile(profile.getName(), "label", profile.getDescription(), provisionerInfo);
      Profile actual = profileService.getProfile(profileId, args);
      Assert.assertEquals(expected, actual);
    } finally {
      profileService.disableProfile(profileId);
      profileService.deleteProfile(profileId);
    }
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
    Profile expected = new Profile("MyProfile", "label", "my profile for testing",
      new ProvisionerInfo("defaultProvisioner", PROPERTY_SUMMARIES));
    // add a profile
    profileService.saveProfile(profileId, expected);

    // get the profile
    Assert.assertEquals(expected, profileService.getProfile(profileId));

    // add a profile which already exists, should succeed and the profile property should be updated
    expected = new Profile("MyProfile", "label", "my 2nd profile for updating",
      new ProvisionerInfo("anotherProvisioner", Collections.emptyList()));
    profileService.saveProfile(profileId, expected);
    Assert.assertEquals(expected, profileService.getProfile(profileId));

    // add another profile to default namespace
    ProfileId profileId2 = NamespaceId.DEFAULT.profile("MyProfile2");
    Profile profile2 = new Profile("MyProfile2", "label", "my 2nd profile for testing",
      new ProvisionerInfo("anotherProvisioner", PROPERTY_SUMMARIES));
    profileService.saveProfile(profileId2, profile2);

    // add default profile
    profileService.saveProfile(ProfileId.NATIVE, Profile.NATIVE);

    // get all profiles
    List<Profile> profiles = ImmutableList.of(expected, profile2, Profile.NATIVE);
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
    profileService.disableProfile(profileId);
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

  @Test
  public void testAddDeleteAssignments() throws Exception {
    ProfileId myProfile = NamespaceId.DEFAULT.profile("MyProfile");
    Profile profile1 = new Profile("MyProfile", Profile.NATIVE.getLabel(), Profile.NATIVE.getDescription(),
                                   Profile.NATIVE.getScope(), Profile.NATIVE.getProvisioner());
    profileService.saveProfile(myProfile, profile1);

    // add a profile assignment and verify
    Set<EntityId> expected = new HashSet<>();
    expected.add(NamespaceId.DEFAULT);
    profileService.addProfileAssignment(myProfile, NamespaceId.DEFAULT);
    Assert.assertEquals(expected, profileService.getProfileAssignments(myProfile));

    // add more and verify
    InstanceId instanceId = new InstanceId("");
    ApplicationId myApp = NamespaceId.DEFAULT.app("myApp");
    ProgramId myProgram = myApp.workflow("myProgram");
    expected.add(instanceId);
    expected.add(myApp);
    expected.add(myProgram);
    profileService.addProfileAssignment(myProfile, instanceId);
    profileService.addProfileAssignment(myProfile, myApp);
    profileService.addProfileAssignment(myProfile, myProgram);
    Assert.assertEquals(expected, profileService.getProfileAssignments(myProfile));

    // add same entity id should not affect
    profileService.addProfileAssignment(myProfile, myApp);
    Assert.assertEquals(expected, profileService.getProfileAssignments(myProfile));

    // delete one and verify
    expected.remove(myApp);
    profileService.removeProfileAssignment(myProfile, myApp);
    Assert.assertEquals(expected, profileService.getProfileAssignments(myProfile));

    // delete all
    for (EntityId entityId : expected) {
      profileService.removeProfileAssignment(myProfile, entityId);
    }
    expected.clear();
    Assert.assertEquals(expected, profileService.getProfileAssignments(myProfile));

    // delete again should not affect
    profileService.removeProfileAssignment(myProfile, myApp);
    Assert.assertEquals(expected, profileService.getProfileAssignments(myProfile));

    profileService.disableProfile(myProfile);
    profileService.deleteProfile(myProfile);
  }

  @Test
  public void testProfileDeletion() throws Exception {
    ProfileId myProfile = NamespaceId.DEFAULT.profile("MyProfile");
    ProfileId myProfile2 = NamespaceId.DEFAULT.profile("MyProfile2");
    Profile profile1 = new Profile("MyProfile", Profile.NATIVE.getLabel(), Profile.NATIVE.getDescription(),
                                   Profile.NATIVE.getScope(), Profile.NATIVE.getProvisioner());
    Profile profile2 = new Profile("MyProfile2", Profile.NATIVE.getLabel(), Profile.NATIVE.getDescription(),
                                   Profile.NATIVE.getScope(), ProfileStatus.DISABLED, Profile.NATIVE.getProvisioner());
    profileService.saveProfile(myProfile, profile1);
    // add profile2 and disable it, profile2 can get deleted at any time
    profileService.saveProfile(myProfile2, profile2);
    profileService.disableProfile(myProfile2);

    // Should not be able to delete because the profile is by default enabled
    try {
      profileService.deleteProfile(myProfile);
      Assert.fail();
    } catch (ProfileConflictException e) {
      // expected
    }

    try {
      profileService.deleteAllProfiles(NamespaceId.DEFAULT);
      Assert.fail();
    } catch (ProfileConflictException e) {
      // expected and check profile 2 is not getting deleted
      Assert.assertEquals(profile2, profileService.getProfile(myProfile2));
    }

    // add assignment and disable it, deletion should also fail
    profileService.addProfileAssignment(myProfile, NamespaceId.DEFAULT);
    profileService.disableProfile(myProfile);

    try {
      profileService.deleteProfile(myProfile);
      Assert.fail();
    } catch (ProfileConflictException e) {
      // expected
    }

    try {
      profileService.deleteAllProfiles(NamespaceId.DEFAULT);
      Assert.fail();
    } catch (ProfileConflictException e) {
      // expected and check profile 2 is not getting deleted
      Assert.assertEquals(profile2, profileService.getProfile(myProfile2));
    }

    profileService.removeProfileAssignment(myProfile, NamespaceId.DEFAULT);

    // add an active record to DefaultStore, deletion should still fail
    Store store = injector.getInstance(DefaultStore.class);
    ProgramId programId = NamespaceId.DEFAULT.app("myApp").workflow("myProgram");
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
    RunId runId = RunIds.generate(System.currentTimeMillis());
    ProgramRunId programRunId = programId.run(runId.getId());
    Map<String, String> systemArgs = Collections.singletonMap(SystemArguments.PROFILE_NAME, myProfile.getScopedName());
    int sourceId = 0;
    store.setProvisioning(programRunId, Collections.emptyMap(), systemArgs,
                          AppFabricTestHelper.createSourceId(++sourceId), artifactId);
    store.setProvisioned(programRunId, 0, AppFabricTestHelper.createSourceId(++sourceId));
    store.setStart(programRunId, null, systemArgs, AppFabricTestHelper.createSourceId(++sourceId));

    try {
      profileService.deleteProfile(myProfile);
      Assert.fail();
    } catch (ProfileConflictException e) {
      // expected
    }

    try {
      profileService.deleteAllProfiles(NamespaceId.DEFAULT);
      Assert.fail();
    } catch (ProfileConflictException e) {
      // expected and check profile 2 is not getting deleted
      Assert.assertEquals(profile2, profileService.getProfile(myProfile2));
    }

    // set the run to stopped then deletion should work
    store.setStop(programRunId, System.currentTimeMillis() + 1000,
                  ProgramController.State.ERROR.getRunStatus(), AppFabricTestHelper.createSourceId(++sourceId));

    // now profile deletion should succeed
    profileService.deleteProfile(myProfile);
    Assert.assertEquals(Collections.singletonList(profile2), profileService.getProfiles(NamespaceId.DEFAULT, false));
    profileService.saveProfile(myProfile, profile1);
    profileService.disableProfile(myProfile);
    profileService.deleteAllProfiles(NamespaceId.DEFAULT);
    Assert.assertEquals(Collections.emptyList(), profileService.getProfiles(NamespaceId.DEFAULT, false));
  }

  @Test
  public void testNativeProfileImmutable() throws Exception {
    // put native profile
    profileService.saveProfile(ProfileId.NATIVE, Profile.NATIVE);

    // save it again should fail since native profile cannot be updated
    try {
      profileService.saveProfile(ProfileId.NATIVE, Profile.NATIVE);
      Assert.fail();
    } catch (MethodNotAllowedException e) {
      // expected
    }

    // native profile cannot be disabled
    try {
      profileService.disableProfile(ProfileId.NATIVE);
      Assert.fail();
    } catch (MethodNotAllowedException e) {
      // expected
    }

    // native profile cannot be deleted
    try {
      profileService.deleteProfile(ProfileId.NATIVE);
      Assert.fail();
    } catch (MethodNotAllowedException e) {
      // expected
    }

    // do not allow delete all profiles in system namespace
    try {
      profileService.deleteAllProfiles(NamespaceId.SYSTEM);
      Assert.fail();
    } catch (MethodNotAllowedException e) {
      // expected
    }
  }

  @Test
  public void testProfileCreationTime() throws Exception {
    ProfileId myProfile = NamespaceId.DEFAULT.profile("MyProfile");
    long creationTime = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    Profile profile = new Profile("MyProfile", Profile.NATIVE.getLabel(), Profile.NATIVE.getDescription(),
                                  Profile.NATIVE.getScope(), ProfileStatus.ENABLED, Profile.NATIVE.getProvisioner(),
                                  creationTime);
    profileService.saveProfile(myProfile, profile);
    Assert.assertEquals(creationTime, profileService.getProfile(myProfile).getCreatedTsSeconds());
  }

  @Test
  public void testProfileMetricsDeletion() throws Exception {
    ProfileId myProfile = NamespaceId.DEFAULT.profile("MyProfile");
    Profile profile = new Profile("MyProfile", Profile.NATIVE.getLabel(), Profile.NATIVE.getDescription(),
                                  Profile.NATIVE.getScope(), Profile.NATIVE.getProvisioner());
    ProgramRunId runId = NamespaceId.DEFAULT.app("myApp").workflow("myProgram").run(RunIds.generate());

    // create and disable the profile
    profileService.saveProfile(myProfile, profile);
    profileService.disableProfile(myProfile);

    // emit some metrics
    MetricsCollectionService metricService = injector.getInstance(MetricsCollectionService.class);
    MetricsContext metricsContext = metricService.getContext(getMetricsTags(runId, myProfile));
    metricsContext.increment(Constants.Metrics.Program.PROGRAM_NODE_MINUTES, 30L);

    MetricStore metricStore = injector.getInstance(MetricStore.class);
    Tasks.waitFor(30L, () -> getMetric(metricStore, runId, myProfile,
                                      "system." + Constants.Metrics.Program.PROGRAM_NODE_MINUTES),
                  10, TimeUnit.SECONDS);

    // delete and verify the metrics are gone
    profileService.deleteProfile(myProfile);
    Tasks.waitFor(0L, () -> getMetric(metricStore, runId, myProfile,
                                       "system." + Constants.Metrics.Program.PROGRAM_NODE_MINUTES),
                  10, TimeUnit.SECONDS);
  }

  private Map<String, String> getMetricsTags(ProgramRunId programRunId, ProfileId profileId) {
    return ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.PROFILE_SCOPE, profileId.getScope().name())
      .put(Constants.Metrics.Tag.PROFILE, profileId.getProfile())
      .put(Constants.Metrics.Tag.NAMESPACE, programRunId.getNamespace())
      .put(Constants.Metrics.Tag.PROGRAM_TYPE, programRunId.getType().getPrettyName())
      .put(Constants.Metrics.Tag.APP, programRunId.getApplication())
      .put(Constants.Metrics.Tag.PROGRAM, programRunId.getProgram())
      .put(Constants.Metrics.Tag.RUN_ID, programRunId.getRun())
      .build();
  }

  private long getMetric(MetricStore metricStore, ProgramRunId programRunId, ProfileId profileId, String metricName) {
    Map<String, String> tags = getMetricsTags(programRunId, profileId);

    MetricDataQuery query = new MetricDataQuery(0, 0, Integer.MAX_VALUE, metricName, AggregationFunction.SUM,
                                                tags, new ArrayList<>());
    Collection<MetricTimeSeries> result = metricStore.query(query);
    if (result.isEmpty()) {
      return 0;
    }
    List<TimeValue> timeValues = result.iterator().next().getTimeValues();
    if (timeValues.isEmpty()) {
      return 0;
    }
    return timeValues.get(0).getValue();
  }
}
