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

import com.google.gson.JsonObject;
import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.bootstrap.BootstrapStepResult;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link AppCreator}
 */
public class AppCreatorTest {
  private static AppCreator appCreator;

  @BeforeClass
  public static void setupClass() {
    Injector injector = AppFabricTestHelper.getInjector();
    appCreator = injector.getInstance(AppCreator.class);
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }

  @Test
  public void testMissingFieldsThrowsException() {
    ArtifactSummary artifactSummary = new ArtifactSummary("name", "1.0.0");
    AppRequest<JsonObject> appRequest = new AppRequest<>(artifactSummary);
    AppCreator.Arguments arguments = new AppCreator.Arguments(appRequest, null, "app", false);
    try {
      arguments.validate();
      Assert.fail("arguments should have been invalidated.");
    } catch (IllegalArgumentException e) {
      // expected
    }
    arguments = new AppCreator.Arguments(appRequest, "namespace", null, false);
    try {
      arguments.validate();
      Assert.fail("arguments should have been invalidated.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void invalidStructureFails() throws Exception {
    JsonObject argumentsObj = new JsonObject();
    argumentsObj.addProperty("namespace", "ns");
    argumentsObj.addProperty("name", "app");
    argumentsObj.addProperty("artifact", "name-1.0.0");
    BootstrapStepResult result = appCreator.execute("label", argumentsObj);
    Assert.assertEquals(BootstrapStepResult.Status.FAILED, result.getStatus());
  }
}
