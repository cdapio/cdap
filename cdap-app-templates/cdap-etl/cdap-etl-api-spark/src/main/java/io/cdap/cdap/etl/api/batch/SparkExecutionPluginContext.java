/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.batch;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.data.batch.Split;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.spark.dynamic.SparkInterpreter;
import io.cdap.cdap.etl.api.TransformContext;
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Context passed to spark plugin types.
 */
@Beta
public interface SparkExecutionPluginContext extends DatasetContext, TransformContext {

  /**
   * Returns the logical start time of the Batch Job.  Logical start time is the time when this Batch
   * job is supposed to start if this job is started by the scheduler. Otherwise it would be the current time when the
   * job runs.
   *
   * @return Time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC).
   */
  @Override
  long getLogicalStartTime();

  /**
   * Returns runtime arguments of the Batch Job.
   *
   * @return runtime arguments of the Batch Job.
   */
  Map<String, String> getRuntimeArguments();

  /**
   * Creates a {@link JavaPairRDD} from the given {@link Dataset}.
   *
   * @param datasetName name of the Dataset
   * @param <K> key type
   * @param <V> value type
   * @return A new {@link JavaPairRDD} instance that reads from the given Dataset
   * @throws DatasetInstantiationException if the Dataset doesn't exist
   */
  <K, V> JavaPairRDD<K, V> fromDataset(String datasetName);

  /**
   * Creates a {@link JavaPairRDD} from the given {@link Dataset} with the given set of Dataset arguments.
   *
   * @param datasetName name of the Dataset
   * @param arguments arguments for the Dataset
   * @param <K> key type
   * @param <V> value type
   * @return A new {@link JavaPairRDD} instance that reads from the given Dataset
   * @throws DatasetInstantiationException if the Dataset doesn't exist
   */
  <K, V> JavaPairRDD<K, V> fromDataset(String datasetName, Map<String, String> arguments);

  /**
   * Creates a {@link JavaPairRDD} from the given {@link Dataset} with the given set of Dataset arguments
   * and custom list of {@link Split}s. Each {@link Split} will create a {@link Partition} in the {@link JavaPairRDD}.
   *
   * @param datasetName name of the Dataset
   * @param arguments arguments for the Dataset
   * @param splits list of {@link Split} or {@code null} to use the default splits provided by the Dataset
   * @param <K> key type
   * @param <V> value type
   * @return A new {@link JavaPairRDD} instance that reads from the given Dataset
   * @throws DatasetInstantiationException if the Dataset doesn't exist
   */
  <K, V> JavaPairRDD<K, V> fromDataset(String datasetName, Map<String, String> arguments,
                                       @Nullable Iterable<? extends Split> splits);

  /**
   * Saves the given {@link JavaPairRDD} to the given {@link Dataset}.
   *
   * @param rdd the {@link JavaPairRDD} to be saved
   * @param datasetName name of the Dataset
   * @throws DatasetInstantiationException if the Dataset doesn't exist
   */
  <K, V> void saveAsDataset(JavaPairRDD<K, V> rdd, String datasetName);

  /**
   * Saves the given {@link JavaPairRDD} to the given {@link Dataset} with the given set of Dataset arguments.
   *
   * @param rdd the {@link JavaPairRDD} to be saved
   * @param datasetName name of the Dataset
   * @param arguments arguments for the Dataset
   * @throws DatasetInstantiationException if the Dataset doesn't exist
   */
  <K, V> void saveAsDataset(JavaPairRDD<K, V> rdd, String datasetName, Map<String, String> arguments);

  /**
   * Returns the {@link JavaSparkContext} used during the execution.
   *
   * @return the Spark Context
   */
  JavaSparkContext getSparkContext();

  /**
   * Returns a {@link Serializable} {@link PluginContext} which can be used to request for plugins instances. The
   * instance returned can also be used in Spark program's closures.
   *
   * @return A {@link Serializable} {@link PluginContext}.
   */
  PluginContext getPluginContext();

  /**
   * Creates a new instance of {@link SparkInterpreter} for Scala code compilation and interpretation.
   *
   * @return a new instance of {@link SparkInterpreter}
   * @throws IOException if failed to create a local directory for storing the compiled class files
   */
  SparkInterpreter createSparkInterpreter() throws IOException;
}
