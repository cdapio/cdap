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

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.Field;
import io.cdap.cdap.etl.api.engine.sql.dataset.RecordCollection;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetDescription;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDatasetProducer;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.cdap.etl.api.sql.engine.dataset.SparkRecordCollectionImpl;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Pull Dataset implementation for unit test
 */
public class MockPullProducer implements SQLDatasetProducer {
  private static final Gson GSON = new Gson();

  private final SQLDatasetDescription datasetDescription;
  private final String expected;

  public MockPullProducer(SQLPullRequest pullRequest, String expected) {
    this.expected = expected;
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
    // Create a spark session and write RDD as JSON
    TypeToken<HashSet<StructuredRecord>> typeToken = new TypeToken<HashSet<StructuredRecord>>() { };
    Type setOfStructuredRecordType = typeToken.getType();

    // Read records from JSON and adjust data types
    Set<StructuredRecord> jsonRecords = GSON.fromJson(expected, setOfStructuredRecordType);
    Set<StructuredRecord> records = new HashSet<>();
    for (StructuredRecord jsonRecord : jsonRecords) {
      records.add(transform(jsonRecord, jsonRecord.getSchema()));
    }

    // Build RDD and generate a new Recrd Collection.
    SparkContext sc = SparkContext.getOrCreate();
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
    SQLContext sqlContext = new SQLContext(sc);
    StructType sparkSchema = DataFrames.toDataType(this.datasetDescription.getSchema());
    JavaRDD<Row> rdd = jsc.parallelize(new ArrayList<>(records)).map(sr -> DataFrames.toRow(sr, sparkSchema));
    Dataset<Row> ds = sqlContext.createDataFrame(rdd.rdd(), sparkSchema);
    return new SparkRecordCollectionImpl(ds);
  }

  /**
   * Structured Record entries read using GSON will contain Doubles instead of Integers.
   *
   * This functions builds a new Structured Record with the correct data type.
   * @param record the structured record we need to adjust
   * @param schema Schema to use
   * @return a new Structured record with Double/Float values adjusted into integers.
   */
  public StructuredRecord transform(StructuredRecord record, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    for (Schema.Field field : schema.getFields()) {
      Object value = record.get(field.getName());

      // Adjust doubles and floats into integers.
      if (value instanceof Double) {
         value = ((Double) value).intValue();
      }
      if (value instanceof Float) {
        value = ((Float) value).intValue();
      }

      builder.set(field.getName(), value);
    }

    return builder.build();
  }
}
