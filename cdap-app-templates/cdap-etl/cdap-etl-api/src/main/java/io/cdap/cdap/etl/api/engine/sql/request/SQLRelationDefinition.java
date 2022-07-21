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
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetDescription;

import java.io.Serializable;

/**
 * This class defines a SQL Engine relation during relationalial transform calls
 */
public class SQLRelationDefinition implements SQLDatasetDescription, Serializable {
  private static final long serialVersionUID = 2528081290251538913L;
  protected final String datasetName;
  protected final Schema datasetSchema;

  public SQLRelationDefinition(String datasetName, Schema datasetSchema) {
    this.datasetName = datasetName;
    this.datasetSchema = datasetSchema;
  }

  /**
   *
   * @return SQL Engine dataset name for a relation
   */
  public String getDatasetName() {
    return datasetName;
  }

  /**
   *
   * @return schema for the relation
   */
  public Schema getSchema() {
    return datasetSchema;
  }
}
