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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.relational.Relation;
import java.io.Serializable;
import java.util.Map;

/**
 * Defines a request to perform relational transformation by SQL engine
 */
public class SQLTransformRequest implements Serializable {

  private final Map<String, SQLDataset> inputDataSets;

  private final String outputDatasetName;
  private final Relation outputRelation;
  private final Schema outputSchema;

  public SQLTransformRequest(Map<String, SQLDataset> inputDataSets, String outputDatasetName,
      Relation outputRelation, Schema outputDataSetSchema) {
    this.inputDataSets = inputDataSets;
    this.outputDatasetName = outputDatasetName;
    this.outputRelation = outputRelation;
    this.outputSchema = outputDataSetSchema;
  }

  /**
   * @return map of input datasets used for the output relation
   */
  public Map<String, SQLDataset> getInputDataSets() {
    return inputDataSets;
  }

  /**
   * @return primary output dataset name
   */
  public String getOutputDatasetName() {
    return outputDatasetName;
  }

  /**
   * @return output dataset to transform
   */
  public Relation getOutputRelation() {
    return outputRelation;
  }

  /**
   * @return output SQLDataset schema
   */
  public Schema getOutputSchema() {
    return outputSchema;
  }
}
