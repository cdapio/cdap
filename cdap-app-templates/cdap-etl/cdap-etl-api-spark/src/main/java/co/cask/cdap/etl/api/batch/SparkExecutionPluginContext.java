/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.api.batch;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.stream.StreamEventDecoder;
import co.cask.cdap.etl.api.TransformContext;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
   * Creates a {@link JavaRDD} that represents all events from the given stream.
   *
   * @param streamName name of the stream
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the Stream doesn't exist
   */
  JavaRDD<StreamEvent> fromStream(String streamName);

  /**
   * Creates a {@link JavaRDD} that represents events from the given stream in the given time range.
   *
   * @param streamName name of the stream
   * @param startTime the starting time of the stream to be read in milliseconds (inclusive)
   * @param endTime the ending time of the streams to be read in milliseconds (exclusive)
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the Stream doesn't exist
   */
  JavaRDD<StreamEvent> fromStream(String streamName, long startTime, long endTime);

  /**
   * Creates a {@link JavaPairRDD} that represents all events from the given stream. The key in the
   * resulting {@link JavaPairRDD} is the event timestamp. The stream body will
   * be decoded as the give value type. Currently it supports {@link Text}, {@link String} and {@link ByteWritable}.
   *
   * @param streamName name of the stream
   * @param valueType type of the stream body to decode to
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the Stream doesn't exist
   */
  <V> JavaPairRDD<Long, V> fromStream(String streamName, Class<V> valueType);

  /**
   * Creates a {@link JavaPairRDD} that represents events from the given stream in the given time range.
   * The key in the resulting {@link JavaPairRDD} is the event timestamp.
   * The stream body will be decoded as the give value type.
   * Currently it supports {@link Text}, {@link String} and {@link ByteWritable}.
   *
   * @param streamName name of the stream
   * @param startTime the starting time of the stream to be read in milliseconds (inclusive)
   * @param endTime the ending time of the streams to be read in milliseconds (exclusive)
   * @param valueType type of the stream body to decode to
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the Stream doesn't exist
   */
  <V> JavaPairRDD<Long, V> fromStream(String streamName, long startTime, long endTime, Class<V> valueType);

  /**
   * Creates a {@link JavaPairRDD} that represents events from the given stream in the given time range.
   * Each steam event will be decoded by an instance of the given {@link StreamEventDecoder} class.
   *
   * @param streamName name of the stream
   * @param startTime the starting time of the stream to be read in milliseconds (inclusive)
   * @param endTime the ending time of the streams to be read in milliseconds (exclusive)
   * @param decoderClass the {@link StreamEventDecoder} for decoding {@link StreamEvent}
   * @param keyType the type of the decoded key
   * @param valueType the type of the decoded value
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the Stream doesn't exist
   */
  <K, V> JavaPairRDD<K, V> fromStream(String streamName, long startTime, long endTime,
                                      Class<? extends StreamEventDecoder<K, V>> decoderClass,
                                      Class<K> keyType, Class<V> valueType);

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
}
