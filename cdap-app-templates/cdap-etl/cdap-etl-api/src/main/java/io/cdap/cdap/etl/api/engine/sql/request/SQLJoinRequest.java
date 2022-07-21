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
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.join.JoinDefinition;

import java.io.Serializable;
import java.util.Collection;

/**
 * Class representing a Request to execute as join operation on a SQL engine.
 */
@Beta
public class SQLJoinRequest extends SQLJoinDefinition implements Serializable {
  private static final long serialVersionUID = -5049631486914347507L;
  private final Collection<SQLDataset> inputDatasets;

  public SQLJoinRequest(String datasetName,
                        JoinDefinition joinDefinition,
                        Collection<SQLDataset> inputDatasets) {
    super(datasetName, joinDefinition);
    this.inputDatasets = inputDatasets;
  }

  /**
   * Get the datasets involved in this join operation.
   */
  public Collection<SQLDataset> getInputDatasets() {
    return inputDatasets;
  }
}
