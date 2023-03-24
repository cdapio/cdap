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

package io.cdap.cdap.etl.spark.batch.relation;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.dataset.RecordCollection;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetConsumer;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetDescription;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;
import io.cdap.cdap.etl.api.sql.engine.dataset.SparkRecordCollection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * SQL Push Dataset implementation for SPARK SQL backed datasets.
 * This is used when CDAP RecordCollection needs to be pushed to Spark inorder to run Spark SQL
 */
public class SparkSQLPushConsumer implements SQLDatasetConsumer {
  private final SQLDatasetDescription datasetDescription;

  public SparkSQLPushConsumer(SQLPushRequest pushRequest) {
    this.datasetDescription = new SQLDatasetDescription() {
      @Override
      public String getDatasetName() {
        return pushRequest.getDatasetName();
      }

      @Override
      public Schema getSchema() {
        return pushRequest.getDatasetSchema();
      }
    };
  }

  @Override
  public SQLDatasetDescription getDescription() {
    return this.datasetDescription;
  }

  @Override
  public SQLDataset consume(RecordCollection collection) {
    SparkRecordCollection sparkCollection = (SparkRecordCollection) collection;
    Dataset<Row> ds = sparkCollection.getDataFrame();
    long numRows = ds.count();

    return new SparkSQLDataset(ds, numRows, datasetDescription.getDatasetName(), datasetDescription.getSchema());
  }
}
