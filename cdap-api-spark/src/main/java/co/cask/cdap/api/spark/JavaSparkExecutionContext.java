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
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowToken;
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Spark program execution context. User Spark program can interact with CDAP through this context.
 */
public abstract class JavaSparkExecutionContext implements RuntimeContext, DatasetContext {

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
   * @return the {@link WorkflowToken} associated with the current {@link Workflow},
   * if the {@link Spark} program is executed as a part of the Workflow.
   */
  @Nullable
  public abstract WorkflowToken getWorkflowToken();

  /**
   * Creates a {@link JavaPairRDD} from the given {@link Dataset}.
   *
   * @param datasetName name of the Dataset
   * @param <K> key type
   * @param <V> value type
   * @return A new {@link JavaPairRDD} instance that reads from the given Dataset
   * @throws DatasetInstantiationException if the Dataset doesn't exist
   */
  public <K, V> JavaPairRDD<K, V> fromDataset(String datasetName) {
    return fromDataset(datasetName, Collections.<String, String>emptyMap());
  }

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
  public <K, V> JavaPairRDD<K, V> fromDataset(String datasetName, Map<String, String> arguments) {
    return fromDataset(datasetName, arguments, null);
  }

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
  public abstract <K, V> JavaPairRDD<K, V> fromDataset(String datasetName,
                                                       Map<String, String> arguments,
                                                       @Nullable Iterable<? extends Split> splits);

  /**
   * Creates a {@link JavaPairRDD} that represents all events from the given stream as (timestamp, string body) pair.
   *
   * @param streamName name of the stream
   * @return A new {@link JavaPairRDD} instance with the key as the stream event timestamp and value is the
   *         UTF-8 decoded event body
   * @throws DatasetInstantiationException if the Stream doesn't exist
   */
  public JavaPairRDD<Long, String> fromStreamAsStringPair(String streamName) {
    return fromStreamAsStringPair(streamName, 0, Long.MAX_VALUE);
  }

  /**
   * Creates a {@link JavaPairRDD} that represents events from the given stream as (timestamp, string body) pair
   * in the given time range.
   *
   * @param streamName name of the stream
   * @param startTime the starting time of the stream to be read in milliseconds (inclusive)
   * @param endTime the ending time of the streams to be read in milliseconds (exclusive)
   * @return A new {@link JavaPairRDD} instance that reads from the given stream,
   *         with the stream event timestamp as key and the UTF-8 decoded event body as value
   * @throws DatasetInstantiationException if the Stream doesn't exist
   */
  public JavaPairRDD<Long, String> fromStreamAsStringPair(String streamName, long startTime, long endTime) {
    return fromStream(streamName, startTime, endTime).mapToPair(new PairFunction<StreamEvent, Long, String>() {
      @Override
      public Tuple2<Long, String> call(StreamEvent streamEvent) throws Exception {
        return new Tuple2<>(streamEvent.getTimestamp(),
                            Charset.forName("UTF-8").decode(streamEvent.getBody()).toString());
      }
    });
  }

  /**
   * Creates a {@link JavaRDD} that represents all events from the given stream.
   *
   * @param streamName name of the stream
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the Stream doesn't exist
   */
  public JavaRDD<StreamEvent> fromStream(String streamName) {
    return fromStream(streamName, 0, Long.MAX_VALUE);
  }

  /**
   * Creates a {@link JavaRDD} that represents events from the given stream in the given time range.
   *
   * @param streamName name of the stream
   * @param startTime the starting time of the stream to be read in milliseconds (inclusive)
   * @param endTime the ending time of the streams to be read in milliseconds (exclusive)
   * @return A new {@link JavaRDD} instance that reads from the given stream
   * @throws DatasetInstantiationException if the Stream doesn't exist
   */
  public abstract JavaRDD<StreamEvent> fromStream(String streamName, long startTime, long endTime);

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
