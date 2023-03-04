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

import io.cdap.cdap.support.status.CollectionState;
import io.cdap.cdap.support.status.SupportBundleTaskStatus;
import java.util.Set;

/**
 * Return response for all individual support bundles status under the support bundle folder EX:
 * [{"bundleId":"634cb763-a63c-4097-a61d-cca1fea1ff9e", "bundleStatus":"FINISHED",
 * "supportBundlePipelineStatus":{"systemLogTaskStatus":"FINISHED", "pipelineInfoTaskStatus":"FINISHED",
 * "runtimeInfoTaskStatus":"FINISHED", "runtimeLogTaskStatus":"FINISHED",
 * "vmInfoTaskStatus":"FINISHED"}}, {"bundleId":"046ad1ff-28fe-4a1e-b2c4-cebe9b548208",
 * "bundleStatus":"FINISHED", "supportBundlePipelineStatus":{"systemLogTaskStatus":"FINISHED",
 * "pipelineInfoTaskStatus":"FINISHED", "runtimeInfoTaskStatus":"FINISHED",
 * "runtimeLogTaskStatus":"FINISHED", "vmInfoTaskStatus":"FINISHED"}}]
 */
public class SupportBundleOperationStatus {

  private final String id;
  private final CollectionState bundleStatus;
  private final Set<SupportBundleTaskStatus> taskStatusSet;

  public SupportBundleOperationStatus(String id,
      CollectionState bundleStatus,
      Set<SupportBundleTaskStatus> taskStatusSet) {
    this.id = id;
    this.bundleStatus = bundleStatus;
    this.taskStatusSet = taskStatusSet;
  }
}
