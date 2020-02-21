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

package io.cdap.cdap.api.spark;

import io.cdap.cdap.api.RuntimeContext;
import io.cdap.cdap.api.SchedulableProgramContext;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.TaskLocalizationContext;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.data.batch.Split;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.lineage.field.LineageRecorder;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metadata.MetadataWriter;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.spark.dynamic.SparkInterpreter;
import io.cdap.cdap.api.workflow.WorkflowInfoProvider;
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.tephra.TransactionFailureException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Spark program execution context. User Spark program can interact with CDAP through this context.
 */
@Beta
public abstract class JavaSparkExecutionContextBase implements SchedulableProgramContext, RuntimeContext, Transactional,
                                                               WorkflowInfoProvider, LineageRecorder,
                                                               SecureStore, MetadataReader, MetadataWriter {

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
   * Returns a {@link MessagingContext} which can be used to interact with the transactional
   * messaging system. Currently the returned instance can only be used in the Spark driver process.
   *
   * @return A {@link MessagingContext}
   */
  public abstract MessagingContext getMessagingContext();

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
   * Saves the given {@link JavaPairRDD} to the given {@link Dataset}.
   *
   * @param rdd the {@link JavaPairRDD} to be saved
   * @param namespace the namespace in which the specified dataset is to be saved
   * @param datasetName name of the Dataset
   * @throws DatasetInstantiationException if the Dataset doesn't exist
   */
  public <K, V> void saveAsDataset(JavaPairRDD<K, V> rdd, String namespace, String datasetName) {
    saveAsDataset(rdd, namespace, datasetName, Collections.<String, String>emptyMap());
  }

  /**
   * Saves the given {@link JavaPairRDD} to the given {@link Dataset} with the given set of Dataset arguments.
   *
   * @param rdd the {@link JavaPairRDD} to be saved
   * @param datasetName name of the dataset
   * @param arguments arguments for the dataset
   * @throws DatasetInstantiationException if the dataset doesn't exist
   */
  public abstract <K, V> void saveAsDataset(JavaPairRDD<K, V> rdd, String datasetName, Map<String, String> arguments);

  /**
   * Saves the given {@link JavaPairRDD} to the given {@link Dataset} with the given set of dataset arguments.
   *
   * @param rdd the {@link JavaPairRDD} to be saved
   * @param namespace the namespace in which the specified Dataset is to be saved
   * @param datasetName name of the Dataset
   * @param arguments arguments for the Dataset
   * @throws DatasetInstantiationException if the Dataset doesn't exist
   */
  public abstract <K, V> void saveAsDataset(JavaPairRDD<K, V> rdd, String namespace, String datasetName,
                                            Map<String, String> arguments);

  /**
   * Transactions with a custom timeout are not supported in Spark.
   *
   * @throws TransactionFailureException always
   */
  public abstract void execute(int timeoutInSeconds, TxRunnable runnable) throws TransactionFailureException;

  /**
   * Creates a new instance of {@link SparkInterpreter} for Scala code compilation and interpretation.
   *
   * @return a new instance of {@link SparkInterpreter}
   * @throws IOException if failed to create a local directory for storing the compiled class files
   */
  public abstract SparkInterpreter createInterpreter() throws IOException;

  /**
   * Returns the underlying {@link SparkExecutionContext} used by this object.
   */
  public abstract SparkExecutionContext getSparkExecutionContext();
}
