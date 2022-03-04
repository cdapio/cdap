/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.support.lib;

/**
 * Defines the type of the support bundle task
 */
public enum SupportBundleTaskType {
  SUPPORT_BUNDLE_SYSTEM_LOG_TASK("SupportBundleSystemLogTask"),
  SUPPORT_BUNDLE_PIPELINE_INFO_TASK("SupportBundlePipelineInfoTask"),
  SUPPORT_BUNDLE_RUNTIME_INFO_TASK("SupportBundleRuntimeInfoTask"),
  SUPPORT_BUNDLE_RUNTIME_LOG_TASK("SupportBundlePipelineRunLogTask");

  private final String type;

  SupportBundleTaskType(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }
}
