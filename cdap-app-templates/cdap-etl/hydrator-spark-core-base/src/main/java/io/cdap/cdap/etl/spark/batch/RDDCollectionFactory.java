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

import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.spark.function.FunctionCache;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Factory that creates a {@link RDDCollection<T>}
 */
public class RDDCollectionFactory<T> implements BatchCollectionFactory<T> {

  private final JavaRDD<T> rdd;

  /**
   * Creates JavaRDD-based pull result
   */
  public RDDCollectionFactory(JavaRDD<T> rdd) {
    this.rdd = rdd;
  }

  @Override
  public BatchCollection<T> create(JavaSparkExecutionContext sec, JavaSparkContext jsc,
      SQLContext sqlContext, DatasetContext datasetContext, SparkBatchSinkFactory sinkFactory,
      FunctionCache.Factory functionCacheFactory) {
    return new RDDCollection<>(sec, functionCacheFactory, jsc, sqlContext, datasetContext,
        sinkFactory, rdd);
  }
}
