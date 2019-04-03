/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Represents the configuration of a namespace. This class needs to be GSON serializable.
 */
public class NamespaceConfig {

  public static final String SCHEDULER_QUEUE_NAME = "scheduler.queue.name";
  public static final String ROOT_DIRECTORY = "root.directory";
  public static final String HBASE_NAMESPACE = "hbase.namespace";
  public static final String HIVE_DATABASE = "hive.database";
  public static final String EXPLORE_AS_PRINCIPAL = "explore.as.principal";

  @SerializedName(SCHEDULER_QUEUE_NAME)
  private final String schedulerQueueName;

  @SerializedName(ROOT_DIRECTORY)
  private final String rootDirectory;

  @SerializedName(HBASE_NAMESPACE)
  private final String hbaseNamespace;

  @SerializedName(HIVE_DATABASE)
  private final String hiveDatabase;

  @SerializedName(EXPLORE_AS_PRINCIPAL)
  private final Boolean exploreAsPrincipal;

  private final String principal;
  private final String groupName;
  private final String keytabURI;

  // scheduler queue name is kept non nullable unlike others like root directory, hbase namespace etc for backward
  // compatibility
  public NamespaceConfig(String schedulerQueueName, @Nullable String rootDirectory,
                         @Nullable String hbaseNamespace, @Nullable String hiveDatabase,
                         @Nullable String principal, @Nullable String groupName, @Nullable String keytabURI) {
    this(schedulerQueueName, rootDirectory, hbaseNamespace, hiveDatabase, principal, groupName, keytabURI, true);
  }

  public NamespaceConfig(String schedulerQueueName, @Nullable String rootDirectory,
                         @Nullable String hbaseNamespace, @Nullable String hiveDatabase,
                         @Nullable String principal, @Nullable String groupName, @Nullable String keytabURI,
                         boolean exploreAsPrincipal) {
    this.schedulerQueueName = schedulerQueueName;
    this.rootDirectory = rootDirectory;
    this.hbaseNamespace = hbaseNamespace;
    this.hiveDatabase = hiveDatabase;
    this.principal = principal;
    this.groupName = groupName;
    this.keytabURI = keytabURI;
    this.exploreAsPrincipal = exploreAsPrincipal;
  }

  public String getSchedulerQueueName() {
    return schedulerQueueName;
  }

  @Nullable
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

  public String getGroupName() {
    return groupName;
  }

  @Nullable
  public String getKeytabURI() {
    return keytabURI;
  }

  /**
   * @return the keytab URI without the fragment containing version.
   */
  @Nullable
  public String getKeytabURIWithoutVersion() {
    // try to get the keytab URI version if the keytab URI is not null
    if (keytabURI != null) {
      // find the position of the fragment containing the version in the keytab URI
      int versionIdx = keytabURI.lastIndexOf("#");
      return versionIdx < 0 ? keytabURI : keytabURI.substring(0, versionIdx);
    }
    return keytabURI;
  }

  /**
   * @return the version of the keytab URI or 0 if no version is present in the keytab URI.
   */
  public int getKeytabURIVersion() {
    // try to get the keytab URI version if the keytab URI is not null
    if (keytabURI != null) {
      // find the position where the fragment containing the version in the keytab URI
      int versionIdx = keytabURI.lastIndexOf("#");
      // if the version doesn't exist in the keytab URI, initialize it to 0
      return versionIdx < 0 ? 0 : Integer.parseInt(keytabURI.substring(versionIdx + 1));
    }
    return 0;
  }

  public Boolean isExploreAsPrincipal() {
    return exploreAsPrincipal == null || exploreAsPrincipal;
  }

  public Set<String> getDifference(@Nullable NamespaceConfig other) {
    Set<String> difference = new HashSet<>();
    if (other == null) {
      // nothing to validate
      return difference;
    }

    if (!Objects.equals(this.rootDirectory, other.rootDirectory)) {
      difference.add(ROOT_DIRECTORY);
    }

    if (!Objects.equals(this.hbaseNamespace, other.hbaseNamespace)) {
      difference.add(HBASE_NAMESPACE);
    }

    if (!Objects.equals(this.hiveDatabase, other.hiveDatabase)) {
      difference.add(HIVE_DATABASE);
    }

    if (!Objects.equals(this.principal, other.principal)) {
      difference.add("principal");
    }

    if (!Objects.equals(this.groupName, other.groupName)) {
      difference.add("groupName");
    }
    return difference;
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
      Objects.equals(groupName, other.groupName) &&
      Objects.equals(keytabURI, other.keytabURI) &&
      Objects.equals(exploreAsPrincipal, other.exploreAsPrincipal);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
      schedulerQueueName, rootDirectory, hbaseNamespace, hiveDatabase, principal, groupName, keytabURI,
      exploreAsPrincipal);
  }

  @Override
  public String toString() {
    return "NamespaceConfig{" +
      "schedulerQueueName='" + schedulerQueueName + '\'' +
      ", rootDirectory='" + rootDirectory + '\'' +
      ", hbaseNamespace='" + hbaseNamespace + '\'' +
      ", hiveDatabase='" + hiveDatabase + '\'' +
      ", principal='" + principal + '\'' +
      ", groupName='" + groupName + '\'' +
      ", keytabURI='" + keytabURI + '\'' +
      ", exploreAsPrincipal='" + exploreAsPrincipal + '\'' +
      '}';
  }
}
