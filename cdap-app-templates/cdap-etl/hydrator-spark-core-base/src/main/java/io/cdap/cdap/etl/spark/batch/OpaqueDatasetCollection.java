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

package io.cdap.cdap.etl.spark.batch;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import io.cdap.cdap.etl.spark.join.JoinExpressionRequest;
import io.cdap.cdap.etl.spark.join.JoinRequest;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

/**
 * This is spark DataSet-based collection, but it does not provide any structure to spark
 * as it uses just serializing encoder for it's content. It's pretty useful to replace RDD
 * as converting DataFrame to such collection does not require a terminal operation.
 * Also it's good for regular batch plugins as it usually contains {@link StructuredRecord}
 * that regular plugins are happy to operate with. For any SparkSQL {@link DataframeCollection}
 * should be used as it uses Spark DataFrame underneath that provides spark with full schema
 * and allows to run SparkSQL.
 */
public class OpaqueDatasetCollection<T> extends DatasetCollection<T> {

  private static final Cache<StructType, ExpressionEncoder<Row>> ENCODER_CACHE = CacheBuilder.newBuilder()
                                                                                            .maximumSize(128)
                                                                                            .build();

  private final Dataset<T> dataset;

  private OpaqueDatasetCollection(Dataset<T> dataset,
      JavaSparkExecutionContext sec,
      JavaSparkContext jsc, SQLContext sqlContext,
      DatasetContext datasetContext, SparkBatchSinkFactory sinkFactory,
      FunctionCache.Factory functionCacheFactory) {
    super(sec, jsc, sqlContext, datasetContext, sinkFactory, functionCacheFactory);
    this.dataset = dataset;
    if (Row.class.isAssignableFrom(dataset.exprEnc().clsTag().runtimeClass())) {
      throw new IllegalArgumentException(
          "Opaque collection received dataset of Row (" + dataset.exprEnc().clsTag()
              .runtimeClass() + "). DataframeCollection should be used.");
    }
  }

  @Override
  public Dataset<T> getDataset() {
    return dataset;
  }

  public static <T> OpaqueDatasetCollection<T> fromDataset(Dataset<T> dataset,
      JavaSparkExecutionContext sec,
      JavaSparkContext jsc, SQLContext sqlContext,
      DatasetContext datasetContext, SparkBatchSinkFactory sinkFactory,
      FunctionCache.Factory functionCacheFactory) {
    return new OpaqueDatasetCollection<T>(dataset, sec, jsc, sqlContext, datasetContext,
        sinkFactory, functionCacheFactory);
  }

  public static <T> OpaqueDatasetCollection<T> fromRdd(JavaRDD<T> rdd,
      JavaSparkExecutionContext sec,
      JavaSparkContext jsc, SQLContext sqlContext,
      DatasetContext datasetContext, SparkBatchSinkFactory sinkFactory,
      FunctionCache.Factory functionCacheFactory) {
    boolean useKryoForDatasets = parseUseKryoForDatasets(sec);
    Dataset<T> dataset = sqlContext.createDataset(rdd.rdd(), objectEncoder(useKryoForDatasets));
    return new OpaqueDatasetCollection<T>(dataset, sec, jsc, sqlContext, datasetContext,
        sinkFactory, functionCacheFactory);
  }
  private static boolean parseUseKryoForDatasets(JavaSparkExecutionContext sec) {
    return Boolean.parseBoolean(sec.getRuntimeArguments()
        .getOrDefault(Constants.DATASET_KRYO_ENABLED, Boolean.TRUE.toString()));
  }

  @Override
  public SparkCollection<T> join(JoinRequest joinRequest) {
    return (SparkCollection<T>) toDataframeCollection(
        joinRequest.getLeftSchema()).join(joinRequest);
  }

  @SuppressWarnings("unchecked")
  @Override
  public SparkCollection<T> join(JoinExpressionRequest joinRequest) {
    return (SparkCollection<T>) toDataframeCollection(
        joinRequest.getLeft().getSchema()).join(joinRequest);
  }

  @Override
  public DataframeCollection toDataframeCollection(Schema schema) {
    StructType sparkSchema = DataFrames.toDataType(schema);
    ExpressionEncoder<Row> encoder = getRowEncoder(sparkSchema);
    Dataset<StructuredRecord> ds = (Dataset<StructuredRecord>) getDataset();
    MapFunction<StructuredRecord, Row> converter = r -> DataFrames.toRow(r, sparkSchema);
    return new DataframeCollection(schema, ds.map(converter, encoder),
        sec, jsc, sqlContext, datasetContext, sinkFactory, functionCacheFactory);
  }

  /**
   * This is required to handle breaking changes between spark 3.3.2 (Dataproc 2.1) to 3.5.1 (Dataproc 2.2).
   * And we need to support both.
   * Here we are trying to check if the new method introduced in 3.5.1 exists or not, and based on that we
   * invoke the new method or the old one.
   */
  public static ExpressionEncoder<Row> getRowEncoder(StructType sparkSchema) {

    try {
      return ENCODER_CACHE.get(sparkSchema, () -> {
        StringBuilder errorStrBuilder = new StringBuilder("Failed to load a suitable Encoder dynamically. Errors : ");
        try {
          Method encoderForMethod =  RowEncoder.class.getMethod("encoderFor", StructType.class);
          Object agnosticEncoderObj = encoderForMethod.invoke(null, sparkSchema);

          Method applyMethod = ExpressionEncoder.class.getMethod("apply",
                         Class.forName("org.apache.spark.sql.catalyst.encoders.AgnosticEncoder"));
          return (ExpressionEncoder<Row>)applyMethod.invoke(null, agnosticEncoderObj);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException |
                 ClassNotFoundException e) {
          errorStrBuilder.append(System.lineSeparator()).append(e.getMessage());
        }

        // If code reaches here, meaning it should be spark 3.3.2 or lower.
        try {
          return RowEncoder.apply(sparkSchema);
        } catch (Exception e) {
          errorStrBuilder.append(System.lineSeparator()).append(e.getMessage());
        }
        throw new RuntimeException(errorStrBuilder.toString());
      });
    } catch (ExecutionException e) {
      throw new UncheckedExecutionException(e);
    }
  }
}
