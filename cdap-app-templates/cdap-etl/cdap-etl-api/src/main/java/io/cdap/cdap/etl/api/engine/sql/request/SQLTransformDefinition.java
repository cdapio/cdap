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
import io.cdap.cdap.etl.api.relational.Relation;
import java.io.Serializable;
import java.util.Map;

/**
 * Defines resulting relational transform that is requested by the plugin
 */
public class SQLTransformDefinition implements Serializable {

  private final String outputDatasetName;
  private final Relation outputRelation;
  private final Schema outputSchema;
  private final Map<String, Relation> outputRelations;
  private final Map<String, Schema> outputSchemas;

  public SQLTransformDefinition(String outputDatasetName, Relation outputRelation,
      Schema outputSchema,
      Map<String, Relation> outputRelations,
      Map<String, Schema> outputSchemas) {
    this.outputDatasetName = outputDatasetName;
    this.outputRelation = outputRelation;
    this.outputSchema = outputSchema;
    this.outputRelations = outputRelations;
    this.outputSchemas = outputSchemas;
  }

  /**
   * @return primary output dataset name
   */
  public String getOutputDatasetName() {
    return outputDatasetName;
  }

  /**
   * @return primary output relation
   */
  public Relation getOutputRelation() {
    return outputRelation;
  }

  /**
   * @return primary output dataset schema
   */
  public Schema getOutputSchema() {
    return outputSchema;
  }

  /**
   * @return map of output dataset name to the relation that defines it
   */
  public Map<String, Relation> getOutputRelations() {
    return outputRelations;
  }

  /**
   * @return map of output dataset name to it's schema
   */
  public Map<String, Schema> getOutputSchemas() {
    return outputSchemas;
  }
}
