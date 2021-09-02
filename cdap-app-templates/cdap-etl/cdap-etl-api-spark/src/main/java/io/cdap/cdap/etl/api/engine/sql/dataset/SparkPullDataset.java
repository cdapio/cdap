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

package io.cdap.cdap.etl.api.engine.sql.dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import javax.annotation.Nullable;

/**
 * SQL Dataset which uses a Spark program to generate records. This can be used to extract records from the SQL
 * Engine when this engine offers a Spark-compatible interface.
 *
 * @param <T> the type of the records to pull from the SQL engine
 */
public interface SparkPullDataset<T> extends SQLDataset {

  /**
   * Creates a new RDD
   * @param context Spark context
   * @return RDD containings records read from the SQL engine. If this result is null, this operation cannot be
   * handled by the Spark implementation.
   */
  @Nullable
  JavaRDD<T> create(JavaSparkContext context);

}
