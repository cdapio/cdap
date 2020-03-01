/*
 * Copyright © 2018-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.profile;

import io.cdap.cdap.AppWithSchedule;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.profile.Profile;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Test for profile metadata publish in the handler level
 */
public class ProfileMetadataTest extends AppFabricTestBase {
  private static MetadataSubscriberService metadataSubscriberService;

  @BeforeClass
  public static void setUp() {
    metadataSubscriberService = getInjector().getInstance(MetadataSubscriberService.class);
    metadataSubscriberService.startAndWait();
  }

  @AfterClass
  public static void tearDown() {
    metadataSubscriberService.stopAndWait();
  }

  @Test
  public void testProfileMetadata() throws Exception {
    // create my profile
    ProfileId myProfile = new NamespaceId(TEST_NAMESPACE1).profile("MyProfile");
    putProfile(myProfile, Profile.NATIVE, 200);
    // create my profile 2
    ProfileId myProfile2 = new NamespaceId(TEST_NAMESPACE1).profile("MyProfile2");
    putProfile(myProfile2, Profile.NATIVE, 200);
    // create system profile 3
    ProfileId myProfile3 = NamespaceId.SYSTEM.profile("MyProfile3");
    putSystemProfile(myProfile3.getProfile(), Profile.NATIVE, 200);

    try {
      // deploy an app with schedule
      AppWithSchedule.AppConfig config =
        new AppWithSchedule.AppConfig(true, true, true);

      Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.fromEntityId(TEST_NAMESPACE_META1.getNamespaceId()),
                                                AppWithSchedule.NAME, VERSION1);
      addAppArtifact(artifactId, AppWithSchedule.class);
      AppRequest<? extends Config> request = new AppRequest<>(
        new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), config, null, null, true);

      // deploy should succeed
      ApplicationId defaultAppId = TEST_NAMESPACE_META1.getNamespaceId().app(AppWithSchedule.NAME);
      Assert.assertEquals(200, deploy(defaultAppId, request).getResponseCode());

      ScheduleId scheduleId1 = defaultAppId.schedule(AppWithSchedule.SCHEDULE);
      ScheduleId scheduleId2 = defaultAppId.schedule(AppWithSchedule.SCHEDULE);
      ProgramId programId = defaultAppId.workflow(AppWithSchedule.WORKFLOW_NAME);
      ProgramId mapReduceProgramId = defaultAppId.mr(AppWithSchedule.MAPREDUCE);
      ProgramId sparkProgramId = defaultAppId.spark(AppWithSchedule.SPARK);
      ProgramId workerProgramId = defaultAppId.worker(AppWithSchedule.WORKER);

      // Verify the workflow, schedule, mapreduce, spark, worker has been updated to native profile
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(), () -> getMetadataProperties(programId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(), () -> getMetadataProperties(mapReduceProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(), () -> getMetadataProperties(sparkProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(), () -> getMetadataProperties(workerProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(), () -> getMetadataProperties(scheduleId1).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(), () -> getMetadataProperties(scheduleId2).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // set it through preferences
      setPreferences(getPreferenceURI(TEST_NAMESPACE1),
                     Collections.singletonMap(SystemArguments.PROFILE_NAME, "USER:MyProfile"), 200);

      // Verify the workflow, schedule, mapreduce, spark, worker has been updated to my profile
      Tasks.waitFor(myProfile.getScopedName(), () -> getMetadataProperties(programId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile.getScopedName(), () -> getMetadataProperties(mapReduceProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile.getScopedName(), () -> getMetadataProperties(sparkProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile.getScopedName(), () -> getMetadataProperties(workerProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile.getScopedName(), () -> getMetadataProperties(scheduleId1).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile.getScopedName(), () -> getMetadataProperties(scheduleId2).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // set it at app level through preferences
      setPreferences(getPreferenceURI(TEST_NAMESPACE1, defaultAppId.getApplication()),
                     Collections.singletonMap(SystemArguments.PROFILE_NAME, "USER:MyProfile2"), 200);

      // Verify the workflow, schedule, mapreduce, spark, worker has been updated to my profile 2
      Tasks.waitFor(myProfile2.getScopedName(), () -> getMetadataProperties(programId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile2.getScopedName(), () -> getMetadataProperties(mapReduceProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile2.getScopedName(), () -> getMetadataProperties(sparkProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile2.getScopedName(), () -> getMetadataProperties(workerProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile2.getScopedName(), () -> getMetadataProperties(scheduleId1).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile2.getScopedName(), () -> getMetadataProperties(scheduleId2).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // delete app level pref, metadata should point to ns level
      deletePreferences(getPreferenceURI(TEST_NAMESPACE1, defaultAppId.getApplication()), 200);

      // Verify the workflow, schedule, mapreduce, spark, worker has been updated to my profile
      Tasks.waitFor(myProfile.getScopedName(), () -> getMetadataProperties(programId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile.getScopedName(), () -> getMetadataProperties(mapReduceProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile.getScopedName(), () -> getMetadataProperties(sparkProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile.getScopedName(), () -> getMetadataProperties(workerProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile.getScopedName(), () -> getMetadataProperties(scheduleId1).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile.getScopedName(), () -> getMetadataProperties(scheduleId2).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // delete at ns level should let the program use native profile since no profile is set at instance level
      deletePreferences(getPreferenceURI(TEST_NAMESPACE1), 200);

      // Verify the workflow, schedule, mapreduce, spark, worker has been updated to native profile
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(), () -> getMetadataProperties(programId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(), () -> getMetadataProperties(mapReduceProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(), () -> getMetadataProperties(sparkProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(), () -> getMetadataProperties(workerProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(), () -> getMetadataProperties(scheduleId1).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(), () -> getMetadataProperties(scheduleId2).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // set it at instance level through preferences
      setPreferences(getPreferenceURI(),
                     Collections.singletonMap(SystemArguments.PROFILE_NAME, "SYSTEM:MyProfile3"), 200);

      // Verify the workflow, schedule, mapreduce, spark, worker has been updated to profile 3
      Tasks.waitFor(myProfile3.getScopedName(), () -> getMetadataProperties(programId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile3.getScopedName(), () -> getMetadataProperties(mapReduceProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile3.getScopedName(), () -> getMetadataProperties(sparkProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile3.getScopedName(), () -> getMetadataProperties(workerProgramId).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile3.getScopedName(), () -> getMetadataProperties(scheduleId1).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(myProfile3.getScopedName(), () -> getMetadataProperties(scheduleId2).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      deletePreferences(getPreferenceURI(), 200);
      deleteApp(defaultAppId, 200);

      // Verify the workflow, schedule, mapreduce, spark, worker metadata has been deleted
      Tasks.waitFor(false, () -> getMetadataProperties(programId).containsKey("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(false, () -> getMetadataProperties(mapReduceProgramId).containsKey("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(false, () -> getMetadataProperties(sparkProgramId).containsKey("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(false, () -> getMetadataProperties(workerProgramId).containsKey("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(false, () -> getMetadataProperties(scheduleId1).containsKey("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      Tasks.waitFor(false, () -> getMetadataProperties(scheduleId2).containsKey("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    } finally {
      // make sure these profiles always get disabled, otherwise deleting the namespace will fail in @AfterClass
      disableProfile(myProfile, 200);
      disableProfile(myProfile2, 200);
      disableSystemProfile(myProfile3.getProfile(), 200);
      deleteProfile(myProfile, 200);
      deleteProfile(myProfile2, 200);
      deleteSystemProfile(myProfile3.getProfile(), 200);
    }
  }
}
