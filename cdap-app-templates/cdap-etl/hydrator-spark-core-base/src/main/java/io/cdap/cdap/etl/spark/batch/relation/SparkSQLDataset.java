/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch.relation;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * This class represents a Dataset in Spark. When records are pushed to Spark SQL , then it is converted to
 * SparkSQLDataset and vice versa.
 */
public class SparkSQLDataset implements SQLDataset {
  private Dataset<Row> ds;
  private Long numRows;
  private String datasetName;
  private Schema schema;

  public SparkSQLDataset(Dataset<Row> ds, Long numRows, String datasetName, Schema schema) {
    this.datasetName = datasetName;
    this.ds = ds;
    this.numRows = numRows;
    this.schema = schema;
  }

  public Dataset<Row> getDs() {
    return this.ds;
  }

  @Override
  public long getNumRows() {
    return this.numRows;
  }

  @Override
  public String getDatasetName() {
    return this.datasetName;
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }
}
