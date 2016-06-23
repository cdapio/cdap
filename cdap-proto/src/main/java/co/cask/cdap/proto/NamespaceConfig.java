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
import javax.annotation.Nullable;

/**
 * Represents the configuration of a namespace. This class needs to be GSON serializable.
 */
public class NamespaceConfig {

  @SerializedName("scheduler.queue.name")
  private final String schedulerQueueName;

  @SerializedName("hdfs.directory")
  private final String hdfsDirectory;

  @SerializedName("hbase.namespace")
  private final String hbaseNamespace;

  @SerializedName("hive.database")
  private final String hiveDatabase;

  public NamespaceConfig(@Nullable String schedulerQueueName, @Nullable String hdfsDirectory, @Nullable String
    hbaseNamespace, @Nullable String hiveDatabase) {
    this.schedulerQueueName = schedulerQueueName;
    this.hdfsDirectory = hdfsDirectory;
    this.hbaseNamespace = hbaseNamespace;
    this.hiveDatabase = hiveDatabase;
  }

  @Nullable
  public String getSchedulerQueueName() {
    return schedulerQueueName;
  }

  public String getHdfsDirectory() {
    return hdfsDirectory;
  }

  @Nullable
  public String getHbaseNamespace() {
    return hbaseNamespace;
  }

  @Nullable
  public String getHiveDatabase() {
    return hiveDatabase;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NamespaceConfig other = (NamespaceConfig) o;
    return Objects.equals(schedulerQueueName, other.schedulerQueueName) &&
      Objects.equals(hdfsDirectory, other.hdfsDirectory) && Objects.equals(hbaseNamespace, other.hbaseNamespace) &&
      Objects.equals(hiveDatabase, other.hiveDatabase);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schedulerQueueName, hdfsDirectory, hbaseNamespace, hiveDatabase);
  }

  @Override
  public String toString() {
    return "NamespaceConfig{" +
      "schedulerQueueName='" + schedulerQueueName + '\'' +
      ", hdfsDirectory='" + hdfsDirectory + '\'' +
      ", hbaseNamespace='" + hbaseNamespace + '\'' +
      ", hiveDatabase='" + hiveDatabase + '\'' +
      '}';
  }
}
