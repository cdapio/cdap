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

public class SupportBundlePipelineStatus {
  private CollectionState systemLogTaskStatus;
  private CollectionState pipelineInfoTaskStatus;
  private CollectionState runtimeInfoTaskStatus;
  private CollectionState runtimeLogTaskStatus;

  public void setSystemLogTaskStatus(CollectionState systemLogTaskStatus) {
    this.systemLogTaskStatus = systemLogTaskStatus;
  }

  public void setPipelineInfoTaskStatus(CollectionState pipelineInfoTaskStatus) {
    this.pipelineInfoTaskStatus = pipelineInfoTaskStatus;
  }

  public void setRuntimeInfoTaskStatus(CollectionState runtimeInfoTaskStatus) {
    this.runtimeInfoTaskStatus = runtimeInfoTaskStatus;
  }

  public void setRuntimeLogTaskStatus(CollectionState runtimeLogTaskStatus) {
    this.runtimeLogTaskStatus = runtimeLogTaskStatus;
  }

  public CollectionState getSystemLogTaskStatus() {
    return systemLogTaskStatus;
  }

  public CollectionState getPipelineInfoTaskStatus() {
    return pipelineInfoTaskStatus;
  }

  public CollectionState getRuntimeInfoTaskStatus() {
    return runtimeInfoTaskStatus;
  }

  public CollectionState getRuntimeLogTaskStatus() {
    return runtimeLogTaskStatus;
  }
}
