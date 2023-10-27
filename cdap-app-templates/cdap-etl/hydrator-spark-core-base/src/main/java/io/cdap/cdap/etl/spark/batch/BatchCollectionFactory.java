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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Interface for factories that has all data needed to create a {@link BatchCollection}
 * when provided with various contexts. An instance of this factory is backed by a specific
 * single data set (RDD / Spark DataSet / Spark DataFrame / ...).
 * @see RDDCollectionFactory
 * @see DataframeCollectionFactory
 */
public interface BatchCollectionFactory<T> {

  /**
   * Create new BatchCollection with the data from this factory object using contexts
   * provided in the parameters.
   * @param sec java spark execution context
   * @param jsc java spark context
   * @param sqlContext sql context
   * @param datasetContext dataset context
   * @param sinkFactory sink factory
   * @param functionCacheFactory function cache factory
   * @return specific instance of BatchCollection backed by this factory data.
   */
  BatchCollection<T> create(JavaSparkExecutionContext sec, JavaSparkContext jsc,
      SQLContext sqlContext, DatasetContext datasetContext, SparkBatchSinkFactory sinkFactory,
      FunctionCache.Factory functionCacheFactory);

}
