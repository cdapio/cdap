/*
 * Copyright © 2021 Cask Data, Inc.
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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

/**
 * Class representing a Join Definition to execute on the SQL Engine.
 */
@Beta
public class SQLJoinDefinition implements Serializable {

  private static final long serialVersionUID = 8608629892525128233L;
  protected final String datasetName;
  protected final Schema datasetSchema;
  protected final JoinDefinition joinDefinition;

  public SQLJoinDefinition(String datasetName,
      JoinDefinition joinDefinition) {
    this(datasetName, joinDefinition, Collections.emptyList());
  }

  public SQLJoinDefinition(String datasetName,
      JoinDefinition joinDefinition,
      Collection<SQLDataset> inputDatasets) {
    this.datasetName = datasetName;
    this.datasetSchema = joinDefinition.getOutputSchema();
    this.joinDefinition = joinDefinition;
  }

  /**
   * Get the name of the dataset which contains the result of this operation.
   */
  public String getDatasetName() {
    return datasetName;
  }

  /**
   * Get the schema for the result of this join operation.
   */
  public Schema getDatasetSchema() {
    return datasetSchema;
  }

  /**
   * Get the join definition for this request.
   */
  public JoinDefinition getJoinDefinition() {
    return joinDefinition;
  }
}
