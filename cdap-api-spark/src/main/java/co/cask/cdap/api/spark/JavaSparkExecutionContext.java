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

package co.cask.cdap.api.spark;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.TaskLocalizationContext;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.stream.GenericStreamEventData;
import co.cask.cdap.api.stream.StreamEventDecoder;
import co.cask.cdap.api.workflow.WorkflowInfoProvider;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Spark program execution context. User Spark program can interact with CDAP through this context.
 */
@Beta
public abstract class JavaSparkExecutionContext implements RuntimeContext, Transactional,
                                                           WorkflowInfoProvider, SecureStore {

  /**
   * @return The specification used to configure this {@link Spark} job instance.
   */
  public abstract SparkSpecification getSpecification();

  /**
   * Returns the logical start time of this Spark job. Logical start time is the time when this Spark
   * job is supposed to start if this job is started by the scheduler. Otherwise it would be the current time when the
   * job runs.
   *
   * @return Time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC).
   */
  public abstract long getLogicalStartTime();

  /**
   * Returns a {@link Serializable} {@link ServiceDiscoverer} for Service Discovery in Spark Program which can be
   * passed in Spark program's closures.
   *
   * @return A {@link Serializable} {@link ServiceDiscoverer}
   */
  public abstract ServiceDiscoverer getServiceDiscoverer();

  /**
   * Returns a {@link Serializable} {@link Metrics} which can be used to emit custom metrics from user's {@link Spark}
   * program. This can also be passed in Spark program's closures and workers can emit their own metrics
   *
   * @return {@link Serializable} {@link Metrics} for {@link Spark} programs
   */
  public abstract Metrics getMetrics();

  /**
   * Returns a {@link Serializable} {@link PluginContext} which can be used to request for plugins instances. The
   * instance returned can also be used in Spark program's closures.
   *
   * @return A {@link Serializable} {@link PluginContext}.
   */
  public abstract PluginContext getPluginContext();

  /**
   * Returns a {@link Serializable} {@link SecureStore} which can be used to request for plugins instances. The
   * instance returned can also be used in Spark program's closures.
   *
   * @return A {@link Serializable} {@link SecureStore}.
   */
  public abstract SecureStore getSecureStore();

  /**
   * Returns a {@link Serializable} {@link TaskLocalizationContext} which can be used to retrieve files localized to
   * task containers. The instance returned can also be used in Spark program's closures.
   *
   * @return the {@link TaskLocalizationContext} for the {@link Spark} program
   */
  public abstract TaskLocalizationContext getLocalizationContext();

  /**
   * Creates a {@link JavaPairRDD} from the given {@link Dataset}.
   *
   * @param datasetName name of the dataset
   * @param <K> key type
   * @param <V> value type
   * @return A new {@link JavaPairRDD} instance that reads from the given dataset
   * @throws DatasetInstantiationException if the dataset doesn't exist
   */
  public <K, V> JavaPairRDD<K, V> fromDataset(String datasetName) {
    return fromDataset(datasetName, Collections.<String, String>emptyMap());
  }

  /**
   * Creates a {@link JavaPairRDD} from the given {@link Dataset}.
   *
   * @param namespace namespace in which the dataset exists
   * @param datasetName name of the dataset
   * @param <K> key type
   * @param <V> value type
   * @return A new {@link JavaPairRDD} instance that reads from the given dataset
   * @throws DatasetInstantiationException if the dataset doesn't exist
   */
  public <K, V> JavaPairRDD<K, V> fromDataset(String namespace, String datasetName) {
    return fromDataset(namespace, datasetName, Collections.<String, String>emptyMap());
  }

  /**
   * Creates a {@link JavaPairRDD} from the given {@link Dataset} with the given set of dataset arguments.
   *
   * @param datasetName name of the dataset
   * @param arguments arguments for the dataset
   * @param <K> key type
   * @param <V> value type
   * @return A new {@link JavaPairRDD} instance that reads from the given dataset
   * @throws DatasetInstantiationException if the dataset doesn't exist
   */
  public <K, V> JavaPairRDD<K, V> fromDataset(String datasetName, Map<String, String> arguments) {
    return fromDataset(datasetName, arguments, null);
  }

  /**
   * Creates a {@link JavaPairRDD} from the given {@link Dataset} with the given set of dataset arguments.
   *
   * @param namespace namespace in which the dataset exists
   * @param datasetName name of the dataset
   * @param arguments arguments for the dataset
   * @param <K> key type
   * @param <V> value type
   * @return A new {@link JavaPairRDD} instance that reads from the given dataset
   * @throws DatasetInstantiationException if the dataset doesn't exist
   */
  public <K, V> JavaPairRDD<K, V> fromDataset(String namespace, String datasetName, Map<String, String> arguments) {
    return fromDataset(namespace, datasetName, arguments, null);
  }

  /**
   * Creates a {@link JavaPairRDD} from the given {@link Dataset} with the given set of dataset arguments
   * and custom list of {@link Split}s. Each {@link Split} will create a {@link Partition} in the {@link JavaPairRDD}.
   *
   * @param datasetName name of the dataset
   * @param arguments arguments for the dataset
   * @param splits list of {@link Split} or {@code null} to use the default splits provided by the dataset
   * @param <K> key type
   * @param <V> value type
   * @return A new {@link JavaPairRDD} instance that reads from the given dataset
   * @throws DatasetInstantiationException if the dataset doesn't exist
   */
  public abstract <K, V> JavaPairRDD<K, V> fromDataset(String datasetName,
                                                       Map<String, String> arguments,
                                                       @Nullable Iterable<? extends Split> splits);

  /**
   * Creates a {@link JavaPairRDD} from the given {@link Dataset} with the given set of dataset arguments
   * and custom list of {@link Split}s. Each {@link Split} will create a {@link Partition} in the {@link JavaPairRDD}.
   *
   * @param namespace namespace in which the dataset exists
   * @param datasetName name of the dataset
   * @param arguments arguments for the dataset
   * @param splits list of {@link Split} or {@code null} to use the default splits provided by the dataset
   * @param <K> key type
   * @param <V> value type
   * @return A new {@link JavaPairRDD} instance that reads from the given dataset
   * @throws DatasetInstantiationException if the dataset doesn't exist
   */
  public abstract <K, V> JavaPairRDD<K, V> fromDataset(String namespace, String datasetName,
                                                       Map<String, String> arguments,
                                                       @Nullable Iterable<? extends Split> splits);


  /**
   * Creates a {@link JavaRDD} that represents all events from the given stream.
   *
   * @param streamName name of the stream
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the stream doesn't exist
   */
  public JavaRDD<StreamEvent> fromStream(String streamName) {
    return fromStream(streamName, 0, Long.MAX_VALUE);
  }

  /**
   * Creates a {@link JavaRDD} that represents all events from the given stream.
   *
   * @param namespace namespace in which the stream exists
   * @param streamName name of the stream
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the stream doesn't exist
   */
  public JavaRDD<StreamEvent> fromStream(String namespace, String streamName) {
    return fromStream(namespace, streamName, 0, Long.MAX_VALUE);
  }

  /**
   * Creates a {@link JavaRDD} that represents events from the given stream in the given time range.
   *
   * @param streamName name of the stream
   * @param startTime the starting time of the stream to be read in milliseconds (inclusive);
   *                  passing in {@code 0} means start reading from the first event available in the stream.
   * @param endTime the ending time of the streams to be read in milliseconds (exclusive);
   *                passing in {@link Long#MAX_VALUE} means read up to latest event available in the stream.
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the stream doesn't exist
   */
  public abstract JavaRDD<StreamEvent> fromStream(String streamName, long startTime, long endTime);

  /**
   * Creates a {@link JavaRDD} that represents events from the given stream in the given time range.
   *
   * @param namespace namespace in which the stream exists
   * @param streamName name of the stream
   * @param startTime the starting time of the stream to be read in milliseconds (inclusive);
   *                  passing in {@code 0} means start reading from the first event available in the stream.
   * @param endTime the ending time of the streams to be read in milliseconds (exclusive);
   *                passing in {@link Long#MAX_VALUE} means read up to latest event available in the stream.
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the stream doesn't exist
   */
  public abstract JavaRDD<StreamEvent> fromStream(String namespace, String streamName, long startTime, long endTime);

  /**
   * Creates a {@link JavaPairRDD} that represents all events from the given stream. The key in the
   * resulting {@link JavaPairRDD} is the event timestamp. The stream body will
   * be decoded as the give value type. Currently it supports {@link Text}, {@link String} and {@link ByteWritable}.
   *
   * @param streamName name of the stream
   * @param valueType type of the stream body to decode to
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the stream doesn't exist
   */
  public <V> JavaPairRDD<Long, V> fromStream(String streamName, Class<V> valueType) {
    return fromStream(streamName, 0, Long.MAX_VALUE, valueType);
  }

  /**
   * Creates a {@link JavaPairRDD} that represents all events from the given stream. The key in the
   * resulting {@link JavaPairRDD} is the event timestamp. The stream body will
   * be decoded as the give value type. Currently it supports {@link Text}, {@link String} and {@link ByteWritable}.
   *
   * @param namespace namespace in which the stream exists
   * @param streamName name of the stream
   * @param valueType type of the stream body to decode to
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the stream doesn't exist
   */
  public <V> JavaPairRDD<Long, V> fromStream(String namespace, String streamName, Class<V> valueType) {
    return fromStream(namespace, streamName, 0, Long.MAX_VALUE, valueType);
  }

  /**
   * Creates a {@link JavaPairRDD} that represents events from the given stream in the given time range.
   * The key in the resulting {@link JavaPairRDD} is the event timestamp.
   * The stream body will be decoded as the give value type.
   * Currently it supports {@link Text}, {@link String} and {@link ByteWritable}.
   *
   * @param streamName name of the stream
   * @param startTime the starting time of the stream to be read in milliseconds (inclusive);
   *                  passing in {@code 0} means start reading from the first event available in the stream.
   * @param endTime the ending time of the streams to be read in milliseconds (exclusive);
   *                passing in {@link Long#MAX_VALUE} means read up to latest event available in the stream.
   * @param valueType type of the stream body to decode to
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the stream doesn't exist
   */
  public abstract <V> JavaPairRDD<Long, V> fromStream(String streamName, long startTime, long endTime,
                                                      Class<V> valueType);

  /**
   * Creates a {@link JavaPairRDD} that represents events from the given stream in the given time range.
   * The key in the resulting {@link JavaPairRDD} is the event timestamp.
   * The stream body will be decoded as the give value type.
   * Currently it supports {@link Text}, {@link String} and {@link ByteWritable}.
   *
   * @param namespace namespace in which the stream exists
   * @param streamName name of the stream
   * @param startTime the starting time of the stream to be read in milliseconds (inclusive);
   *                  passing in {@code 0} means start reading from the first event available in the stream.
   * @param endTime the ending time of the streams to be read in milliseconds (exclusive);
   *                passing in {@link Long#MAX_VALUE} means read up to latest event available in the stream.
   * @param valueType type of the stream body to decode to
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the stream doesn't exist
   */
  public abstract <V> JavaPairRDD<Long, V> fromStream(String namespace, String streamName, long startTime, long endTime,
                                                      Class<V> valueType);

  /**
   * Creates a {@link JavaPairRDD} that represents events from the given stream in the given time range.
   * Each steam event will be decoded by an instance of the given {@link StreamEventDecoder} class.
   *
   * @param streamName name of the stream
   * @param startTime the starting time of the stream to be read in milliseconds (inclusive);
   *                  passing in {@code 0} means start reading from the first event available in the stream.
   * @param endTime the ending time of the streams to be read in milliseconds (exclusive);
   *                passing in {@link Long#MAX_VALUE} means read up to latest event available in the stream.
   * @param decoderClass the {@link StreamEventDecoder} for decoding {@link StreamEvent}
   * @param keyType the type of the decoded key
   * @param valueType the type of the decoded value
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the stream doesn't exist
   */
  public abstract <K, V> JavaPairRDD<K, V> fromStream(String streamName, long startTime, long endTime,
                                                      Class<? extends StreamEventDecoder<K, V>> decoderClass,
                                                      Class<K> keyType, Class<V> valueType);

  /**
   * Creates a {@link JavaPairRDD} that represents events from the given stream in the given time range.
   * Each steam event will be decoded by an instance of the given {@link StreamEventDecoder} class.
   *
   * @param namespace namespace in which the stream exists
   * @param streamName name of the stream
   * @param startTime the starting time of the stream to be read in milliseconds (inclusive);
   *                  passing in {@code 0} means start reading from the first event available in the stream.
   * @param endTime the ending time of the streams to be read in milliseconds (exclusive);
   *                passing in {@link Long#MAX_VALUE} means read up to latest event available in the stream.
   * @param decoderClass the {@link StreamEventDecoder} for decoding {@link StreamEvent}
   * @param keyType the type of the decoded key
   * @param valueType the type of the decoded value
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the stream doesn't exist
   */
  public abstract <K, V> JavaPairRDD<K, V> fromStream(String namespace, String streamName, long startTime, long endTime,
                                                      Class<? extends StreamEventDecoder<K, V>> decoderClass,
                                                      Class<K> keyType, Class<V> valueType);

  /**
   * Creates a {@link JavaPairRDD} that represents all events from the given stream.
   * The first entry in the pair is a {@link Long}, representing the
   * event timestamp, while the second entry is a {@link GenericStreamEventData},
   * which contains data decoded from the stream event body base on
   * the given {@link FormatSpecification}.
   *
   * @param streamName name of the stream
   * @param formatSpec the {@link FormatSpecification} describing the format in the stream
   * @param <T> value type
   * @return a new {@link JavaPairRDD} instance that reads from the given stream.
   * @throws DatasetInstantiationException if the Stream doesn't exist
   */
  public <T> JavaPairRDD<Long, GenericStreamEventData<T>> fromStream(String streamName,
                                                                     FormatSpecification formatSpec,
                                                                     Class<T> dataType) {
    return fromStream(streamName, formatSpec, 0, Long.MAX_VALUE, dataType);
  }

  /**
   * Creates a {@link JavaPairRDD} that represents all events from the given stream.
   * The first entry in the pair is a {@link Long}, representing the
   * event timestamp, while the second entry is a {@link GenericStreamEventData},
   * which contains data decoded from the stream event body base on
   * the given {@link FormatSpecification}.
   *
   * @param namespace namespace in which the stream exists
   * @param streamName name of the stream
   * @param formatSpec the {@link FormatSpecification} describing the format in the stream
   * @param <T> value type
   * @return a new {@link JavaPairRDD} instance that reads from the given stream.
   * @throws DatasetInstantiationException if the Stream doesn't exist
   */
  public <T> JavaPairRDD<Long, GenericStreamEventData<T>> fromStream(String namespace, String streamName,
                                                                     FormatSpecification formatSpec,
                                                                     Class<T> dataType) {
    return fromStream(namespace, streamName, formatSpec, 0, Long.MAX_VALUE, dataType);
  }

  /**
   * Creates a {@link JavaPairRDD} that represents data from the given stream for events in the given
   * time range. The first entry in the pair is a {@link Long}, representing the
   * event timestamp, while the second entry is a {@link GenericStreamEventData},
   * which contains data decoded from the stream event body base on
   * the given {@link FormatSpecification}.
   *
   * @param streamName name of the stream
   * @param formatSpec the {@link FormatSpecification} describing the format in the stream
   * @param startTime the starting time of the stream to be read in milliseconds (inclusive);
   *                  passing in {@code 0} means start reading from the first event available in the stream.
   * @param endTime the ending time of the streams to be read in milliseconds (exclusive);
   *                passing in {@link Long#MAX_VALUE} means read up to latest event available in the stream.
   * @param <T> value type
   * @return a new {@link JavaPairRDD} instance that reads from the given stream.
   * @throws DatasetInstantiationException if the Stream doesn't exist
   */
  public abstract <T> JavaPairRDD<Long, GenericStreamEventData<T>> fromStream(String streamName,
                                                                              FormatSpecification formatSpec,
                                                                              long startTime, long endTime,
                                                                              Class<T> dataType);

  /**
   * Creates a {@link JavaPairRDD} that represents data from the given stream for events in the given
   * time range. The first entry in the pair is a {@link Long}, representing the
   * event timestamp, while the second entry is a {@link GenericStreamEventData},
   * which contains data decoded from the stream event body base on
   * the given {@link FormatSpecification}.
   *
   * @param namespace namespace in which the stream exists
   * @param streamName name of the stream
   * @param formatSpec the {@link FormatSpecification} describing the format in the stream
   * @param startTime the starting time of the stream to be read in milliseconds (inclusive);
   *                  passing in {@code 0} means start reading from the first event available in the stream.
   * @param endTime the ending time of the streams to be read in milliseconds (exclusive);
   *                passing in {@link Long#MAX_VALUE} means read up to latest event available in the stream.
   * @param <T> value type
   * @return a new {@link JavaPairRDD} instance that reads from the given stream.
   * @throws DatasetInstantiationException if the Stream doesn't exist
   */
  public abstract <T> JavaPairRDD<Long, GenericStreamEventData<T>> fromStream(String namespace, String streamName,
                                                                              FormatSpecification formatSpec,
                                                                              long startTime, long endTime,
                                                                              Class<T> dataType);

  /**
   * Saves the given {@link JavaPairRDD} to the given {@link Dataset}.
   *
   * @param rdd the {@link JavaPairRDD} to be saved
   * @param datasetName name of the Dataset
   * @throws DatasetInstantiationException if the Dataset doesn't exist
   */
  public <K, V> void saveAsDataset(JavaPairRDD<K, V> rdd, String datasetName) {
    saveAsDataset(rdd, datasetName, Collections.<String, String>emptyMap());
  }

  /**
   * Saves the given {@link JavaPairRDD} to the given {@link Dataset} with the given set of Dataset arguments.
   *
   * @param rdd the {@link JavaPairRDD} to be saved
   * @param datasetName name of the Dataset
   * @param arguments arguments for the Dataset
   * @throws DatasetInstantiationException if the Dataset doesn't exist
   */
  public abstract <K, V> void saveAsDataset(JavaPairRDD<K, V> rdd, String datasetName, Map<String, String> arguments);
}
