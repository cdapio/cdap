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
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.proto.bootstrap.BootstrapStepResult;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link ProgramStarter}
 */
public class ProgramStarterTest {
  private static ProgramStarter programStarter;

  @BeforeClass
  public static void setupClass() {
    Injector injector = AppFabricTestHelper.getInjector();
    programStarter = injector.getInstance(ProgramStarter.class);
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }

  @Test
  public void testMissingFieldsThrowsException() {
    ProgramStarter.Arguments arguments = new ProgramStarter.Arguments(null, "app", "WORKFLOW", "name");
    try {
      arguments.validate();
      Assert.fail("arguments should have been invalidated.");
    } catch (IllegalArgumentException e) {
      // expected
    }
    arguments = new ProgramStarter.Arguments("ns", null, "WORKFLOW", "name");
    try {
      arguments.validate();
      Assert.fail("arguments should have been invalidated.");
    } catch (IllegalArgumentException e) {
      // expected
    }
    arguments = new ProgramStarter.Arguments("ns", "app", null, "name");
    try {
      arguments.validate();
      Assert.fail("arguments should have been invalidated.");
    } catch (IllegalArgumentException e) {
      // expected
    }
    arguments = new ProgramStarter.Arguments("ns", "app", "WORKFLOW", null);
    try {
      arguments.validate();
      Assert.fail("arguments should have been invalidated.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidProgramTypeThrowsException() {
    ProgramStarter.Arguments arguments = new ProgramStarter.Arguments("ns", "app", "PORKFLOW", "name");
    arguments.validate();
  }

  @Test
  public void invalidStructureFails() throws Exception {
    JsonObject argumentsObj = new JsonObject();
    argumentsObj.addProperty("namespace", "ns");
    argumentsObj.addProperty("application", "app");
    argumentsObj.addProperty("type", "WORKFLOW");
    argumentsObj.add("name", new JsonObject());
    BootstrapStepResult result = programStarter.execute("label", argumentsObj);
    Assert.assertEquals(BootstrapStepResult.Status.FAILED, result.getStatus());
  }
}
