/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link RuntimeMonitors}.
 */
public class RuntimeMonitorsTest {

  @Test
  public void testTrimSystemArgsKeepsRuntimeScopes() {
    String scopesJson = "{\"connections\":[\"c1\"],\"oauth\":[],\"secrets\":[\"s1\"],"
        + "\"hasExternalDynamicSecret\":false}";
    Map<String, String> input = ImmutableMap.of(
        SystemArguments.PROFILE_NAME, "system:dataproc",
        ProgramOptionConstants.PRINCIPAL, "alice",
        ProgramOptionConstants.RUNTIME_SCOPES, scopesJson,
        "other.system.arg", "dropped");

    Map<String, String> trimmed = RuntimeMonitors.trimSystemArgs(input);

    Assert.assertEquals(ImmutableMap.of(
        SystemArguments.PROFILE_NAME, "system:dataproc",
        ProgramOptionConstants.PRINCIPAL, "alice",
        ProgramOptionConstants.RUNTIME_SCOPES, scopesJson), trimmed);
  }

  @Test
  public void testTrimSystemArgsOmitsAbsentRuntimeScopes() {
    Map<String, String> input = ImmutableMap.of(
        SystemArguments.PROFILE_NAME, "system:dataproc",
        "other.system.arg", "dropped");

    Map<String, String> trimmed = RuntimeMonitors.trimSystemArgs(input);

    Assert.assertEquals(ImmutableMap.of(SystemArguments.PROFILE_NAME, "system:dataproc"), trimmed);
    Assert.assertFalse(trimmed.containsKey(ProgramOptionConstants.RUNTIME_SCOPES));
  }
}
