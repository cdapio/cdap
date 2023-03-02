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

package io.cdap.cdap.etl.api.engine.sql.request;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineInput;
import java.io.Serializable;

/**
 * A request to perform read operation
 */
@Beta
public class SQLReadRequest implements Serializable {

  private final String datasetName;
  private final SQLEngineInput input;

  private static final long serialVersionUID = 1153836963681453732L;

  /**
   * Creates a new SQLReadRequest instance
   *
   * @param datasetName dataset name
   * @param input SQL engine input configuration
   */
  public SQLReadRequest(String datasetName, SQLEngineInput input) {
    this.datasetName = datasetName;
    this.input = input;
  }

  /**
   * Get the name of the dataset that should be read from the source
   */
  public String getDatasetName() {
    return datasetName;
  }

  /**
   * Describes the input source
   */
  public SQLEngineInput getInput() {
    return input;
  }
}
