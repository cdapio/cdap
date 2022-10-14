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

package io.cdap.cdap.spi.metadata;

import io.cdap.cdap.api.annotation.Beta;

import java.util.Objects;

/**
 * Represents an Asset with a FQN which is a fully-qualified identifier and the location.
 * It refers to the data source that is being read from or written into - e.g. BigQuery Dataset, DB Table, etc.
 * FQN is formed by using the plugin properties that together identifies an asset. For e.g. in case of DB plugins,
 * the plugin can be of the form {dbType}.{host}: {port}.{database}.{schema}.
 * If location is not known for an asset, it is set to "unknown" by default.
 * If project ID is not known for an asset, it is set to "" by default.
 */
@Beta
public class Asset {

  private static final String DEFAULT_LOCATION = "global";
  private static final String DEFAULT_PROJECT = "";

  private final String fqn;
  private final String location;
  private final String projectId;

  /**
   * Creates an instance of Asset. Location will be "unknown" if not set.
   * @param fqn fully-qualified name of the Asset.
   */
  public Asset(String fqn) {
    this(fqn, DEFAULT_LOCATION, DEFAULT_PROJECT);
  }

  /**
   * Creates an instance of Asset.
   * @param fqn fully-qualified name of the Asset.
   * @param location location of the Asset.
   */
  public Asset(String fqn, String location) {
    this(fqn, location, DEFAULT_PROJECT);
  }

  /**
   * Creates an instance of Asset.
   * @param fqn fully-qualified name of the Asset.
   * @param location location of the Asset.
   * @param projectId project for the Asset.
   */
  public Asset(String fqn, String location, String projectId) {
    this.fqn = fqn;
    this.location = location;
    this.projectId = projectId;
  }

  /**
   * @return the fully-qualified name of the {@link Asset}.
   */
  public String getFQN() {
    return fqn;
  }

  /**
   * @return the location of the {@link Asset}.
   */
  public String getLocation() {
    return location;
  }

  /**
   * @return the project ID of the {@link Asset}, if applicable.
   */
  public String getProjectId() {
    return projectId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Asset asset = (Asset) o;
    return fqn.equals(asset.fqn) && location.equals(asset.location) && projectId.equals(asset.projectId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fqn, location, projectId);
  }

  @Override
  public String toString() {
    return "Dataset{" +
      "fqn='" + fqn + '\'' +
      ", location='" + location + '\'' +
      ", projectId='" + projectId + '\'' +
      '}';
  }
}
