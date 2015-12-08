/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.ClientLocalizationContext;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.TaskLocalizationContext;
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.DatasetProvider;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.stream.StreamEventDecoder;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowToken;

import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Spark job execution context. This context is shared between CDAP and User's Spark job.
 * This interface exposes two prominent methods:
 * <ol>
 * <li>{@link SparkContext#readFromDataset(String, Class, Class)}: Allows user to read a {@link Dataset} as an
 * RDD</li>
 * <li>{@link SparkContext#writeToDataset(Object, String, Class, Class)}: Allows user to write a RDD to a {@link
 * Dataset}</li>
 * </ol>
 * Classes implementing this interface should also have a Spark Context member of appropriate type on which these
 * method acts.
 */
@Beta
public interface SparkContext extends RuntimeContext, DatasetProvider, ClientLocalizationContext {
  /**
   * @return The specification used to configure this {@link Spark} job instance.
   */
  SparkSpecification getSpecification();

  /**
   * Returns the logical start time of this Spark job. Logical start time is the time when this Spark
   * job is supposed to start if this job is started by the scheduler. Otherwise it would be the current time when the
   * job runs.
   *
   * @return Time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC).
   */
  long getLogicalStartTime();

  /**
   * Create a Spark RDD that uses {@link Dataset} as input source
   *
   * @param datasetName the name of the {@link Dataset} to be read as an RDD
   * @param kClass      the key class
   * @param vClass      the value class
   * @param <T>         type of RDD
   * @return the RDD created from Dataset
   * @throws UnsupportedOperationException if the SparkContext is not yet initialized
   */
  <T> T readFromDataset(String datasetName, Class<?> kClass, Class<?> vClass);

  /**
   * Create a Spark RDD that uses {@link Dataset} instantiated using the provided arguments as an input source.
   *
   * @param datasetName the name of the {@link Dataset} to be read as an RDD
   * @param kClass      the key class
   * @param vClass      the value class
   * @param datasetArgs arguments for the dataset
   * @param <T>         type of RDD
   * @return the RDD created from Dataset
   * @throws UnsupportedOperationException if the SparkContext is not yet initialized
   */
  <T> T readFromDataset(String datasetName, Class<?> kClass, Class<?> vClass, Map<String, String> datasetArgs);

  /**
   * Writes a Spark RDD to {@link Dataset}
   *
   * @param rdd         the rdd to be stored
   * @param datasetName the name of the {@link Dataset} where the RDD should be stored
   * @param kClass      the key class
   * @param vClass      the value class
   * @param <T>         type of RDD
   * @throws UnsupportedOperationException if the SparkContext is not yet initialized
   */
  <T> void writeToDataset(T rdd, String datasetName, Class<?> kClass, Class<?> vClass);

  /**
   * Writes a Spark RDD to {@link Dataset} instantiated using the provided arguments.
   *
   * @param rdd         the rdd to be stored
   * @param datasetName the name of the {@link Dataset} where the RDD should be stored
   * @param kClass      the key class
   * @param vClass      the value class
   * @param datasetArgs arguments for the dataset
   * @param <T>         type of RDD
   * @throws UnsupportedOperationException if the SparkContext is not yet initialized
   */
  <T> void writeToDataset(T rdd, String datasetName, Class<?> kClass, Class<?> vClass, Map<String, String> datasetArgs);

  /**
   * Create a Spark RDD that uses complete {@link Stream} as input source
   *
   * @param streamName the name of the {@link Stream} to be read as an RDD
   * @param vClass     the value class
   * @param <T>        type of RDD
   * @return the RDD created from {@link Stream}
   */
  <T> T readFromStream(String streamName, Class<?> vClass);

  /**
   * Create a Spark RDD that uses {@link Stream} as input source
   *
   * @param streamName the name of the {@link Stream} to be read as an RDD
   * @param vClass     the value class
   * @param startTime  the starting time of the stream to be read in milliseconds. To read from the starting of the
   *                   stream set this to 0
   * @param endTime    the ending time of the streams to be read in milliseconds. To read up to the end of the stream
   *                   set this to Long.MAX_VALUE
   * @param <T>        type of RDD
   * @return the RDD created from {@link Stream}
   */
  <T> T readFromStream(String streamName, Class<?> vClass, long startTime, long endTime);

  /**
   * Create a Spark RDD that uses {@link Stream} as input source according to the given {@link StreamEventDecoder}
   *
   * @param streamName  the name of the {@link Stream} to be read as an RDD
   * @param vClass      the value class
   * @param startTime   the starting time of the stream to be read in milliseconds. To read from the starting of the
   *                    stream set this to 0
   * @param endTime     the ending time of the streams to be read in milliseconds. To read up to the end of the stream
   *                    set this to Long.MAX_VALUE
   * @param decoderType the decoder to use while reading streams
   * @param <T>         type of RDD
   * @return the RDD created from {@link Stream}
   */
  <T> T readFromStream(String streamName, Class<?> vClass, long startTime, long endTime,
                       Class<? extends StreamEventDecoder> decoderType);

  /**
   * Create a Spark RDD that uses {@link Stream} as input source according to the given {@link StreamBatchReadable}.
   *
   * @param stream a {@link StreamBatchReadable} containing information on the stream to read from
   * @param vClass the value class
   * @return the RDD created from {@link Stream}
   */
   <T> T readFromStream(StreamBatchReadable stream, Class<?> vClass);

  /**
   * Returns
   * <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.api.java.JavaSparkContext">
   * JavaSparkContext</a> or
   * <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkContext">SparkContext</a>
   * depending on user's job type.
   *
   * @param <T> the type of Spark Context
   * @return the Spark Context
   */
  <T> T getOriginalSparkContext();

  /**
   * Returns a {@link Serializable} {@link ServiceDiscoverer} for Service Discovery in Spark Program which can be
   * passed in Spark program's closures.
   *
   * @return A {@link Serializable} {@link ServiceDiscoverer}
   */
  ServiceDiscoverer getServiceDiscoverer();

  /**
   * Returns a {@link Serializable} {@link Metrics} which can be used to emit custom metrics from user's {@link Spark}
   * program. This can also be passed in Spark program's closures and workers can emit their own metrics
   *
   * @return {@link Serializable} {@link Metrics} for {@link Spark} programs
   */
  Metrics getMetrics();

  /**
   * Returns a {@link Serializable} {@link PluginContext} which can be used to request for plugins instances. The
   * instance returned can also be used in Spark program's closures.
   *
   * @return A {@link Serializable} {@link PluginContext}.
   */
  PluginContext getPluginContext();

  /**
   * Override the resources, such as memory and virtual cores, to use for each executor process for the Spark program.
   * This method should be called in {@link Spark#beforeSubmit(SparkContext)} to take effect.
   *
   * @param resources Resources that each executor should use
   */
  void setExecutorResources(Resources resources);

  /**
   * Sets a
   * <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkConf">SparkConf</a>
   * to be used for the Spark execution. Only configurations set inside the
   * {@link Spark#beforeSubmit(SparkContext)} call will affect the Spark execution.
   *
   * @param <T> the SparkConf type
   */
  <T> void setSparkConf(T sparkConf);

  /**
   * @return the {@link WorkflowToken} associated with the current {@link Workflow},
   * if the {@link Spark} program is executed as a part of the Workflow.
   */
  @Nullable
  WorkflowToken getWorkflowToken();

  /**
   * Returns a {@link Serializable} {@link TaskLocalizationContext} which can be used to retrieve files localized to
   * task containers. The instance returned can also be used in Spark program's closures.
   *
   * @return the {@link TaskLocalizationContext} for the {@link Spark} program
   */
  TaskLocalizationContext getTaskLocalizationContext();
}
