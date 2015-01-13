/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib;

import java.util.Map;

/**
 * Represents one partition of a {@link co.cask.cdap.api.dataset.lib.Partitioned} dataset.
 */
public final class Partition {

  private final String datasetName;
  private final Map<String, String> datasetArguments;
  private final Map<String, Object> metadata;

  public Partition(String datasetName, Map<String, String> datasetArguments, Map<String, Object> metadata) {
    this.datasetName = datasetName;
    this.datasetArguments = datasetArguments;
    this.metadata = metadata;
  }

  /**
   * The name of the dataset that holds this partition.
   * @return the name of the dataset.
   * TODO with namespaces we will need to use an Id.Dataset that has namespace and name.
   */
  public String getDatasetName() {
    return datasetName;
  }

  /**
   * Runtime arguments for the dataset.
   * @return A map of runtime arguments.
   */
  public Map<String, String> getArguments() {
    return datasetArguments;
  }

  /**
   * The meta data that uniquely identifies this partition.
   * @return A map from field name to value.
   */
  public Map<String, Object> getMetadata() {
    return metadata;
  }
}
