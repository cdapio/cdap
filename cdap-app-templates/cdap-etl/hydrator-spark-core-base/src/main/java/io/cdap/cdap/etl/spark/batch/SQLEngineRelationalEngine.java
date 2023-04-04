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

package io.cdap.cdap.etl.spark.batch;

import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.RelationalTransform;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.SparkCollectionRelationalEngine;
import io.cdap.cdap.etl.spark.function.CountingFunction;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.util.Map;
import java.util.Optional;

/**
 * Spark Collection relational engine wrapper that tries to tranform with SQL Engine
 */
public class SQLEngineRelationalEngine implements SparkCollectionRelationalEngine {
  private final JavaSparkExecutionContext sec;
  private final FunctionCache.Factory functionCacheFactory;
  private final JavaSparkContext jsc;
  private final SQLContext sqlContext;
  private final DatasetContext datasetContext;
  private final SparkBatchSinkFactory sinkFactory;
  private final BatchSQLEngineAdapter adapter;

  public SQLEngineRelationalEngine(JavaSparkExecutionContext sec,
                                   FunctionCache.Factory functionCacheFactory,
                                   JavaSparkContext jsc,
                                   SQLContext sqlContext,
                                   DatasetContext datasetContext,
                                   SparkBatchSinkFactory sinkFactory,
                                   BatchSQLEngineAdapter adapter) {
    this.sec = sec;
    this.functionCacheFactory = functionCacheFactory;
    this.jsc = jsc;
    this.sqlContext = sqlContext;
    this.datasetContext = datasetContext;
    this.sinkFactory = sinkFactory;
    this.adapter = adapter;
  }

  @Override
  public Engine getRelationalEngine() {
    return adapter.getSQLRelationalEngine();
  }

  @Override
  public <T> Optional<SparkCollection<T>> tryRelationalTransform(
    StageSpec stageSpec, RelationalTransform transform, Map<String, SparkCollection<Object>> input) {

     return adapter.tryRelationalTransform(stageSpec, transform, input)
      .map(job -> {
        SQLEngineCollection<T> sqlEngineCollection = new SQLEngineCollection<T>(sec, functionCacheFactory, jsc,
                                                                                sqlContext, datasetContext, sinkFactory,
                                                                                stageSpec.getName(), adapter, job);
        // If it is a Spark Sql Job with a local engine, then Perform a Pull resulting in RddCollection.
        if (adapter.getSQLEngineClassName().equals(SparkSQLEngine.class.getName())) {

          // CDAP-20508 : Inorder to display records in Preview: In case of RDD based operations such as JOINS,
          // we use the `CountingFunction` to push the records via `SparkDataTracer`. While other plugin stages is
          // handled via Emitter.
          if (adapter.isPreviewEnabled()) {
            return sqlEngineCollection.pull().map(new CountingFunction<>(stageSpec.getName(), sec.getMetrics(),
                                                                         Constants.Metrics.RECORDS_OUT,
                                                                         sec.getDataTracer(stageSpec.getName())));
          }
          return sqlEngineCollection.pull();
        }
        return sqlEngineCollection;
      });
  }
}
