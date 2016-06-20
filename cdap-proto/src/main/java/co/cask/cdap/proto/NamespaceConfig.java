/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

/**
 * Represents the configuration of a namespace. This class needs to be GSON serializable.
 */
public class NamespaceConfig {

  @SerializedName("scheduler.queue.name")
  private final String schedulerQueueName;

  @SerializedName("hdfs.user")
  private final String hdfsUser;

  public NamespaceConfig(String schedulerQueueName, String hdfsUser) {
    this.schedulerQueueName = schedulerQueueName;
    this.hdfsUser = hdfsUser;
  }

  public String getSchedulerQueueName() {
    return schedulerQueueName;
  }

  public String getHdfsUser() {
    return hdfsUser;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NamespaceConfig that = (NamespaceConfig) o;

    if (!schedulerQueueName.equals(that.schedulerQueueName)) {
      return false;
    }
    return hdfsUser.equals(that.hdfsUser);

  }

  @Override
  public int hashCode() {
    int result = schedulerQueueName.hashCode();
    result = 31 * result + hdfsUser.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "NamespaceConfig{" +
      "schedulerQueueName='" + schedulerQueueName + '\'' +
      ", hdfsUser='" + hdfsUser + '\'' +
      '}';
  }
}
