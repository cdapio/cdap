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

/**
 * Collection of Support bundle task status
 *
 */
public class SupportBundlePipelineStatus {
  private final CollectionState systemLogTaskStatus;
  private final CollectionState pipelineInfoTaskStatus;
  private final CollectionState runtimeInfoTaskStatus;
  private final CollectionState runtimeLogTaskStatus;
  private final CollectionState vmInfoTaskStatus;

  public SupportBundlePipelineStatus(CollectionState systemLogTaskStatus,
                                     CollectionState pipelineInfoTaskStatus,
                                     CollectionState runtimeInfoTaskStatus,
                                     CollectionState runtimeLogTaskStatus,
                                     CollectionState vmInfoTaskStatus) {
    this.systemLogTaskStatus = systemLogTaskStatus;
    this.pipelineInfoTaskStatus = pipelineInfoTaskStatus;
    this.runtimeInfoTaskStatus = runtimeInfoTaskStatus;
    this.runtimeLogTaskStatus = runtimeLogTaskStatus;
    this.vmInfoTaskStatus = vmInfoTaskStatus;
  }
}
