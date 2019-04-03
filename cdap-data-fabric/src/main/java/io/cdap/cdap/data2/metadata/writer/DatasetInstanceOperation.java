/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.writer;

import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.proto.security.Principal;

import javax.annotation.Nullable;

/**
 * Class for carrying information about operation on a dataset instance. It is used for replicating dataset instance
 * operations happening in remote runtime back to master.
 *
 * Ideally this class can be replaced with the {@link MetadataOperation}. However, due to currently the DatasetService
 * is neither using nor part of metadata system, we need separate class to carry the dataset instance operations.
 */
public final class DatasetInstanceOperation {

  /**
   * Defines the type of operation
   */
  public enum Type {
    CREATE,
    UPDATE,
    DELETE
  }

  private final Type type;
  private final Principal principal;

  @Nullable
  private final String datasetTypeName;
  @Nullable
  private final DatasetProperties properties;

  /**
   * Creates a new instance representing a dataset creation operation.
   *
   * @param principal the user {@link Principal} who initiated the action
   * @param datasetTypeName the name of the dataset type for the new dataset instance
   * @param properties the {@link DatasetProperties} for the new dataset instance
   * @return a {@link DatasetInstanceOperation} representing the creation operation
   */
  public static DatasetInstanceOperation create(Principal principal,
                                                String datasetTypeName, DatasetProperties properties) {
    return new DatasetInstanceOperation(principal, Type.CREATE, datasetTypeName, properties);
  }

  /**
   * Creates a new instance representing a dataset properties update operation.
   *
   * @param principal the user {@link Principal} who initiated the action
   * @param properties the {@link DatasetProperties} to update to
   * @return a {@link DatasetInstanceOperation} representing the update operation
   */
  public static DatasetInstanceOperation update(Principal principal, DatasetProperties properties) {
    return new DatasetInstanceOperation(principal, Type.UPDATE, null, properties);
  }

  /**
   * Creates a new instance representing a dataset deletion operation.
   *
   * @param principal the user {@link Principal} who initiated the action
   * @return a {@link DatasetInstanceOperation} representing the deletion operation
   */
  public static DatasetInstanceOperation delete(Principal principal) {
    return new DatasetInstanceOperation(principal, Type.DELETE, null, null);
  }

  private DatasetInstanceOperation(Principal principal, Type type,
                                   @Nullable String datasetTypeName,
                                   @Nullable DatasetProperties properties) {
    this.principal = principal;
    this.type = type;
    this.datasetTypeName = datasetTypeName;
    this.properties = properties;
  }

  public Principal getPrincipal() {
    return principal;
  }

  public Type getType() {
    return type;
  }

  @Nullable
  public String getDatasetTypeName() {
    return datasetTypeName;
  }

  @Nullable
  public DatasetProperties getProperties() {
    return properties;
  }
}
