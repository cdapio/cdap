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

package io.cdap.cdap.internal.bootstrap;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;

import java.util.Collections;
import java.util.List;

/**
 * Specified operations to perform to bootstrap a CDAP instance.
 */
public class BootstrapConfig {
  public static final BootstrapConfig EMPTY = new BootstrapConfig(Collections.emptyList());
  public static final BootstrapConfig DEFAULT = new BootstrapConfig(ImmutableList.of(
    new BootstrapStep("Create Native Profile", BootstrapStep.Type.CREATE_NATIVE_PROFILE,
                      BootstrapStep.RunCondition.ONCE, new JsonObject()),
    new BootstrapStep("Create Default Namespace", BootstrapStep.Type.CREATE_DEFAULT_NAMESPACE,
                      BootstrapStep.RunCondition.ONCE, new JsonObject()),
    new BootstrapStep("Load System Artifacts", BootstrapStep.Type.LOAD_SYSTEM_ARTIFACTS,
                      BootstrapStep.RunCondition.ALWAYS, new JsonObject())));
  private final List<BootstrapStep> steps;

  public BootstrapConfig(List<BootstrapStep> steps) {
    this.steps = steps;
  }

  public List<BootstrapStep> getSteps() {
    // can be null when deserialized through gson
    return steps == null ? Collections.emptyList() : Collections.unmodifiableList(steps);
  }

  /**
   * Check that this is a valid config, throwing an exception if not.
   *
   * @throws IllegalArgumentException if the config is invalid
   */
  public void validate() {
    for (BootstrapStep step : steps) {
      step.validate();
    }
  }
}
