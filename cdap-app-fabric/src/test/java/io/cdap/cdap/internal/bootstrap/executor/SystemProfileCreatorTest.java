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
 *
 */

package io.cdap.cdap.internal.bootstrap.executor;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Injector;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.internal.provision.MockProvisioner;
import io.cdap.cdap.proto.EntityScope;
import io.cdap.cdap.proto.bootstrap.BootstrapStepResult;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.proto.provisioner.ProvisionerInfo;
import io.cdap.cdap.proto.provisioner.ProvisionerPropertyValue;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link SystemProfileCreator}.
 */
public class SystemProfileCreatorTest {
  private static final Gson GSON = new Gson();
  private static SystemProfileCreator profileCreator;
  private static ProfileService profileService;

  @BeforeClass
  public static void setupClass() {
    Injector injector = AppFabricTestHelper.getInjector();
    profileCreator = injector.getInstance(SystemProfileCreator.class);
    profileService = injector.getInstance(ProfileService.class);
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }

  @After
  public void cleanupTest() {
    profileService.clear();
  }

  @Test
  public void testMissingProfileName() throws Exception {
    List<ProvisionerPropertyValue> properties = new ArrayList<>();
    properties.add(new ProvisionerPropertyValue("name1", "val1", true));
    properties.add(new ProvisionerPropertyValue("name2", "val2", true));
    ProvisionerInfo provisionerInfo = new ProvisionerInfo(MockProvisioner.NAME, properties);
    SystemProfileCreator.Arguments arguments = new SystemProfileCreator.Arguments("", "label", "desc", provisionerInfo);
    BootstrapStepResult result = profileCreator.execute("label", GSON.toJsonTree(arguments).getAsJsonObject());

    Assert.assertEquals(BootstrapStepResult.Status.FAILED, result.getStatus());
  }

  @Test
  public void testMissingProvisionerInfo() throws Exception {
    SystemProfileCreator.Arguments arguments = new SystemProfileCreator.Arguments("name", "label", "desc", null);
    BootstrapStepResult result = profileCreator.execute("label", GSON.toJsonTree(arguments).getAsJsonObject());

    Assert.assertEquals(BootstrapStepResult.Status.FAILED, result.getStatus());
  }

  @Test
  public void testInvalidArgumentStructure() throws Exception  {
    JsonObject arguments = new JsonObject();
    arguments.addProperty("name", "p1");
    arguments.addProperty("description", "desc");
    arguments.addProperty("label", "some label");
    // this is invalid, should be an object
    arguments.addProperty("provisioner", "native");
    BootstrapStepResult result = profileCreator.execute("label", arguments);

    Assert.assertEquals(BootstrapStepResult.Status.FAILED, result.getStatus());
  }

  @Test
  public void testCreation() throws Exception {
    ProfileId profileId = NamespaceId.SYSTEM.profile("p1");
    try {
      profileService.getProfile(profileId);
      Assert.fail("profile should not exist.");
    } catch (NotFoundException e) {
      // expected
    }

    List<ProvisionerPropertyValue> properties = new ArrayList<>();
    properties.add(new ProvisionerPropertyValue("name1", "val1", true));
    properties.add(new ProvisionerPropertyValue("name2", "val2", true));
    ProvisionerInfo provisionerInfo = new ProvisionerInfo(MockProvisioner.NAME, properties);
    Profile profile = new Profile(profileId.getProfile(), "profile label", "profile description",
                                  EntityScope.SYSTEM, provisionerInfo);
    SystemProfileCreator.Arguments arguments = new SystemProfileCreator.Arguments(profile.getName(), profile.getLabel(),
                                                                                  profile.getDescription(),
                                                                                  profile.getProvisioner());
    BootstrapStepResult result = profileCreator.execute("label", GSON.toJsonTree(arguments).getAsJsonObject());
    BootstrapStepResult expected = new BootstrapStepResult("label", BootstrapStepResult.Status.SUCCEEDED);
    Assert.assertEquals(expected, result);
    Assert.assertEquals(profile, profileService.getProfile(profileId));
  }

  @Test
  public void testExistingIsUnmodified() throws Exception {
    // write a profile
    ProfileId profileId = NamespaceId.SYSTEM.profile("p1");
    List<ProvisionerPropertyValue> properties = new ArrayList<>();
    properties.add(new ProvisionerPropertyValue("name1", "val1", true));
    properties.add(new ProvisionerPropertyValue("name2", "val2", true));
    ProvisionerInfo provisionerInfo = new ProvisionerInfo(MockProvisioner.NAME, properties);
    Profile profile = new Profile(profileId.getProfile(), "profile label", "profile description",
                                  EntityScope.SYSTEM, provisionerInfo);
    profileService.saveProfile(profileId, profile);

    // run the bootstrap step and make sure it succeeded
    SystemProfileCreator.Arguments arguments = new SystemProfileCreator.Arguments(profile.getName(), "different label",
                                                                                  "different desciption",
                                                                                  profile.getProvisioner());
    BootstrapStepResult result = profileCreator.execute("label", GSON.toJsonTree(arguments).getAsJsonObject());
    BootstrapStepResult expected = new BootstrapStepResult("label", BootstrapStepResult.Status.SUCCEEDED);
    Assert.assertEquals(expected, result);

    // check that it didn't overwrite the existing profile
    Assert.assertEquals(profile, profileService.getProfile(profileId));
  }
}
