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
import com.google.inject.Injector;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.proto.bootstrap.BootstrapStepResult;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link SystemPreferenceSetter}
 */
public class SystemPreferencesSetterTest {
  private static final Gson GSON = new Gson();
  private static SystemPreferenceSetter systemPreferenceSetter;
  private static PreferencesService preferencesService;

  @BeforeClass
  public static void setupClass() {
    Injector injector = AppFabricTestHelper.getInjector();
    systemPreferenceSetter = injector.getInstance(SystemPreferenceSetter.class);
    preferencesService = injector.getInstance(PreferencesService.class);
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }

  @After
  public void cleanupTest() {
    preferencesService.deleteProperties();
  }

  @Test
  public void testAllPreferenceSet() throws Exception {
    Map<String, String> preferences = new HashMap<>();
    preferences.put("p1", "v1");
    preferences.put("p2", "v2");

    SystemPreferenceSetter.Arguments arguments = new SystemPreferenceSetter.Arguments(preferences);
    BootstrapStepResult result = systemPreferenceSetter.execute("label", GSON.toJsonTree(arguments).getAsJsonObject());
    Assert.assertEquals(BootstrapStepResult.Status.SUCCEEDED, result.getStatus());
    Assert.assertEquals(preferences, preferencesService.getProperties());
  }

  @Test
  public void testExistingIsUnmodified() throws Exception {
    Map<String, String> preferences = new HashMap<>();
    preferences.put("p1", "v1");
    preferencesService.setProperties(preferences);
    preferences.put("p2", "v2");
    preferences.put("p1", "v3");

    SystemPreferenceSetter.Arguments arguments = new SystemPreferenceSetter.Arguments(preferences);
    BootstrapStepResult result = systemPreferenceSetter.execute("label", GSON.toJsonTree(arguments).getAsJsonObject());
    Assert.assertEquals(BootstrapStepResult.Status.SUCCEEDED, result.getStatus());

    Map<String, String> expected = new HashMap<>();
    // p1 should not have been overridden
    expected.put("p1", "v1");
    expected.put("p2", "v2");
    Assert.assertEquals(expected, preferencesService.getProperties());
  }
}
