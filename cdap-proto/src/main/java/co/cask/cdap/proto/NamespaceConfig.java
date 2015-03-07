/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.proto;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

/**
 * Represents the configuration of a namespace. This class needs to be GSON serializable.
 */
public class NamespaceConfig {

  @SerializedName("scheduler.queue.name")
  private final String schedulerQueueName;

  public NamespaceConfig(String schedulerQueueName) {
    this.schedulerQueueName = schedulerQueueName;
  }

  public String getSchedulerQueueName() {
    return schedulerQueueName;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("scheduler.queue.name", schedulerQueueName)
                  .toString();
  }
}
