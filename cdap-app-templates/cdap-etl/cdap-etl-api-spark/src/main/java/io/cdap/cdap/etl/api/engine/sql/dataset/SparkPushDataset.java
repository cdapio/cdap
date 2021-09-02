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

/**
 * SQL Dataset which uses a Spark program to consume records. This can be used to store records in the SQL
 * Engine when this engine offers a Spark-compatible interface.
 *
 * @param <T> the type of the records to push into the SQL engine
 */
public interface SparkPushDataset<T> extends SQLDataset {

  /**
   * Consumes a Spark RDD and stores the records in the SQL Engine.
   * @param rdd Spark RDD to store.
   * @return boolean specifying if this operation could be performed with Spark or not.
   */
  boolean consume(JavaRDD<T> rdd);

}
