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
import javax.ws.rs.HEAD;

/**
 * Represents the configuration of a namespace. This class needs to be GSON serializable.
 */
public class NamespaceConfig {

  public static final String SCHEDULER_QUEUE_NAME = "scheduler.queue.name";
  public static final String ROOT_DIRECTORY = "root.directory";
  public static final String HBASE_NAMESPACE = "hbase.namespace";
  public static final String HIVE_DATABASE = "hive.database";

  @SerializedName(SCHEDULER_QUEUE_NAME)
  private final String schedulerQueueName;

  @SerializedName(ROOT_DIRECTORY)
  private final String rootDirectory;

  @SerializedName(HBASE_NAMESPACE)
  private final String hbaseNamespace;

  @SerializedName(HIVE_DATABASE)
  private final String hiveDatabase;

  private final String principal;
  private final String keytabURI;

  // scheduler queue name is kept non nullable unlike others like root directory, hbase namespace etc for backward
  // compatibility
  public NamespaceConfig(String schedulerQueueName, @Nullable String rootDirectory,
                         @Nullable String hbaseNamespace, @Nullable String hiveDatabase,
                         @Nullable String principal, @Nullable String keytabURI) {
    this.schedulerQueueName = schedulerQueueName;
    this.rootDirectory = rootDirectory;
    this.hbaseNamespace = hbaseNamespace;
    this.hiveDatabase = hiveDatabase;
    this.principal = principal;
    this.keytabURI = keytabURI;
  }

  public String getSchedulerQueueName() {
    return schedulerQueueName;
  }

  public String getRootDirectory() {
    return rootDirectory;
  }

  @Nullable
  public String getHbaseNamespace() {
    return hbaseNamespace;
  }

  @Nullable
  public String getHiveDatabase() {
    return hiveDatabase;
  }

  @Nullable
  public String getPrincipal() {
    return principal;
  }

  @Nullable
  public String getKeytabURI() {
    return keytabURI;
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
      Objects.equals(rootDirectory, other.rootDirectory) &&
      Objects.equals(hbaseNamespace, other.hbaseNamespace) &&
      Objects.equals(hiveDatabase, other.hiveDatabase) &&
      Objects.equals(principal, other.principal) &&
      Objects.equals(keytabURI, other.keytabURI);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schedulerQueueName, rootDirectory, hbaseNamespace, hiveDatabase, principal, keytabURI);
  }

  @Override
  public String toString() {
    return "NamespaceConfig{" +
      "schedulerQueueName='" + schedulerQueueName + '\'' +
      ", rootDirectory='" + rootDirectory + '\'' +
      ", hbaseNamespace='" + hbaseNamespace + '\'' +
      ", hiveDatabase='" + hiveDatabase + '\'' +
      ", principal='" + principal + '\'' +
      ", keytabURI='" + keytabURI + '\'' +
      '}';
  }
}
