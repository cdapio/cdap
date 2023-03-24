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
import io.cdap.cdap.etl.api.engine.sql.dataset.RecordCollection;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetDescription;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetProducer;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.cdap.etl.api.sql.engine.dataset.SparkRecordCollectionImpl;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * SQL Pull Dataset implementation for SPARK SQL backed datasets.
 * This is used when Spark Dataset is pulled out of Spark to CDAP RecordCollection
 */
public class SparkSQLPullProducer implements SQLDatasetProducer {
  private final SQLDatasetDescription datasetDescription;

  public SparkSQLPullProducer(SQLPullRequest pullRequest) {
    this.datasetDescription = new SQLDatasetDescription() {
      @Override
      public String getDatasetName() {
        return pullRequest.getDatasetName();
      }

      @Override
      public Schema getSchema() {
        return pullRequest.getDatasetSchema();
      }
    };
  }

  @Override
  public SQLDatasetDescription getDescription() {
    return this.datasetDescription;
  }

  @Override
  public RecordCollection produce(SQLDataset dataset) {
    SparkSQLDataset sparkSQLDataset = (SparkSQLDataset) dataset;
    Dataset<Row> ds = sparkSQLDataset.getDs();
    RecordCollection recordCollection = new SparkRecordCollectionImpl(ds);
    return recordCollection;
  }
}
