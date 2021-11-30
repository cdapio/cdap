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

package io.cdap.cdap.etl.mock.batch;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.dataset.RecordCollection;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetConsumer;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetDescription;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;
import io.cdap.cdap.etl.api.sql.engine.dataset.SparkRecordCollection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.nio.file.FileSystems;

/**
 * Pull Dataset implementation for unit test
 */
public class MockPushConsumer implements SQLDatasetConsumer {
  private final SQLDatasetDescription datasetDescription;
  private final String dirName;

  public MockPushConsumer(SQLPushRequest pushRequest, String dirName) {
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
    this.dirName = dirName + FileSystems.getDefault().getSeparator() + pushRequest.getDatasetName();
  }

  @Override
  public SQLDatasetDescription getDescription() {
    return this.datasetDescription;
  }

  @Override
  public SQLDataset consume(RecordCollection collection) {
    SparkRecordCollection sparkCollection = (SparkRecordCollection) collection;

    // Create a spark session and write RDD as JSON
    Dataset<Row> ds = sparkCollection.getDataFrame();
    long numRows = ds.count();
    ds.write().json(dirName);

    return new SQLDataset() {
      @Override
      public long getNumRows() {
        return numRows;
      }

      @Override
      public String getDatasetName() {
        return datasetDescription.getDatasetName();
      }

      @Override
      public Schema getSchema() {
        return datasetDescription.getSchema();
      }
    };
  }
}
