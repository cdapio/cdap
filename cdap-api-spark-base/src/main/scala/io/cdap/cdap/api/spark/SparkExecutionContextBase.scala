/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.api.spark

import java.io.IOException

import io.cdap.cdap.api.annotation.Beta
import io.cdap.cdap.api.data.batch.Split
import io.cdap.cdap.api.lineage.field.LineageRecorder
import io.cdap.cdap.api.messaging.MessagingContext
import io.cdap.cdap.api.metadata.{MetadataReader, MetadataWriter}
import io.cdap.cdap.api.metrics.Metrics
import io.cdap.cdap.api.plugin.PluginContext
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo
import io.cdap.cdap.api.security.store.SecureStore
import io.cdap.cdap.api.spark.dynamic.SparkInterpreter
import io.cdap.cdap.api.workflow.{WorkflowInfo, WorkflowToken}
import io.cdap.cdap.api.{RuntimeContext, ServiceDiscoverer, TaskLocalizationContext, Transactional, TxRunnable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.tephra.TransactionFailureException

import scala.reflect.ClassTag
/**
  * Spark program execution context. User Spark program can interact with CDAP through this context.
  */
@Beta
trait SparkExecutionContextBase extends RuntimeContext
  with Transactional with MetadataReader with MetadataWriter with LineageRecorder {

  /**
    * @return The specification used to configure this Spark job instance.
    */
  def getSpecification: SparkSpecification

  /**
    * Returns the logical start time of this Spark job. Logical start time is the time when this Spark
    * job is supposed to start if this job is started by the scheduler. Otherwise it would be the current time when the
    * job runs.
    *
    * @return Time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC).
    */
  def getLogicalStartTime: Long

  /**
    * Returns a [[scala.Serializable]] [[io.cdap.cdap.api.ServiceDiscoverer]] for Service Discovery
    * in Spark Program which can be passed in Spark program's closures.
    *
    * @return A [[scala.Serializable]] [[io.cdap.cdap.api.ServiceDiscoverer]]
    */
  def getServiceDiscoverer: ServiceDiscoverer

  /**
    * Returns a [[scala.Serializable]] [[io.cdap.cdap.api.metrics.Metrics]] which can be used to emit
    * custom metrics from the Spark program. This can also be passed in closures and workers can emit their own metrics.
    *
    * @return A [[scala.Serializable]] [[io.cdap.cdap.api.metrics.Metrics]]
    */
  def getMetrics: Metrics

  /**
    * Returns a [[scala.Serializable]] [[io.cdap.cdap.api.plugin.PluginContext]] which can be used to request
    * for plugins instances. The instance returned can also be used in Spark program's closures.
    *
    * @return A [[scala.Serializable]] [[io.cdap.cdap.api.plugin.PluginContext]]
    */
  def getPluginContext: PluginContext

  /**
    * Returns a [[scala.Serializable]] [[io.cdap.cdap.api.security.store.SecureStore]] which can be used to request
    * for plugins instances. The instance returned can also be used in Spark program's closures.
    *
    * @return A [[scala.Serializable]] [[io.cdap.cdap.api.plugin.PluginContext]]
    */
  def getSecureStore: SecureStore

  /**
    * Returns a [[io.cdap.cdap.api.messaging.MessagingContext]] which can be used to interact with the transactional
    * messaging system. Currently the returned instance can only be used in the Spark driver process.
    *
    * @return A [[io.cdap.cdap.api.messaging.MessagingContext]]
    */
  def getMessagingContext: MessagingContext

  /**
    * Returns the [[io.cdap.cdap.api.workflow.WorkflowToken]] if the Spark program
    * is started from a [[io.cdap.cdap.api.workflow.Workflow]].
    *
    * @return An optional [[io.cdap.cdap.api.workflow.WorkflowToken]] associated with
    *         the current [[io.cdap.cdap.api.workflow.Workflow]]
    */
  def getWorkflowToken: Option[WorkflowToken]

  /**
    * Returns the [[io.cdap.cdap.api.workflow.WorkflowInfo]] if the Spark program
    * is started from a [[io.cdap.cdap.api.workflow.Workflow]].
    *
    * @return An optional [[io.cdap.cdap.api.workflow.WorkflowInfo]] associated with
    *         the current [[io.cdap.cdap.api.workflow.Workflow]]
    */
  def getWorkflowInfo: Option[WorkflowInfo]

  /**
    * Returns the [[io.cdap.cdap.api.TaskLocalizationContext]] that gives access to files that were localized
    * by [[io.cdap.cdap.api.spark.Spark]] `beforeSubmit` method.
    */
  def getLocalizationContext: TaskLocalizationContext

  /**
    * Creates a [[org.apache.spark.rdd.RDD]] from the given [[io.cdap.cdap.api.dataset.Dataset]].
    * Using the implicit object io.cdap.cdap.api.spark.SparkMain.SparkProgramContextFunctions is preferred.
    *
    * @param sc the [[org.apache.spark.SparkContext]] to use
    * @param datasetName name of the Dataset
    * @param arguments arguments for the Dataset
    * @param splits an [[scala.Option]] of [[scala.collection.Iterable]] of [[io.cdap.cdap.api.data.batch.Split]]
    *               to be used to compute the partitions of the RDD
    * @tparam K key type
    * @tparam V value type
    * @return A new [[org.apache.spark.rdd.RDD]] instance that reads from the given Dataset.
    * @throws io.cdap.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
    */
  def fromDataset[K: ClassTag, V: ClassTag](sc: SparkContext,
                                            datasetName: String,
                                            arguments: Map[String, String],
                                            splits: Option[Iterable[_ <: Split]]): RDD[(K, V)]

  /**
    * Creates a [[org.apache.spark.rdd.RDD]] from the given [[io.cdap.cdap.api.dataset.Dataset]].
    * Using the implicit object io.cdap.cdap.api.spark.SparkMain.SparkProgramContextFunctions is preferred.
    *
    * @param sc the [[org.apache.spark.SparkContext]] to use
    * @param namespace namespace in which the dataset exists
    * @param datasetName name of the Dataset
    * @param arguments arguments for the Dataset
    * @param splits an [[scala.Option]] of [[scala.collection.Iterable]] of [[io.cdap.cdap.api.data.batch.Split]]
    *               to be used to compute the partitions of the RDD
    * @tparam K key type
    * @tparam V value type
    * @return A new [[org.apache.spark.rdd.RDD]] instance that reads from the given Dataset.
    * @throws io.cdap.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
    */
  def fromDataset[K: ClassTag, V: ClassTag](sc: SparkContext,
                                            namespace: String,
                                            datasetName: String,
                                            arguments: Map[String, String],
                                            splits: Option[Iterable[_ <: Split]]): RDD[(K, V)]

  /**
    * Saves the given [[org.apache.spark.rdd.RDD]] to the given [[io.cdap.cdap.api.dataset.Dataset]].
    * Using the implicit object io.cdap.cdap.api.spark.SparkMain.SparkProgramRDDFunctions is preferred.
    *
    * @param rdd the [[org.apache.spark.rdd.RDD]] to operate on
    * @param datasetName name of the Dataset
    * @param arguments arguments for the Dataset
    * @throws io.cdap.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
    */
  def saveAsDataset[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)],
                                              datasetName: String, arguments: Map[String, String]): Unit

  /**
    * Saves the given [[org.apache.spark.rdd.RDD]] to the given [[io.cdap.cdap.api.dataset.Dataset]].
    * Using the implicit object io.cdap.cdap.api.spark.SparkMain.SparkProgramRDDFunctions is preferred.
    *
    * @param rdd the [[org.apache.spark.rdd.RDD]] to operate on
    * @param namespace namespace for the Dataset
    * @param datasetName name of the Dataset
    * @param arguments arguments for the Dataset
    * @throws io.cdap.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
    */
  def saveAsDataset[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], namespace: String, datasetName: String,
                                              arguments: Map[String, String]): Unit

  /**
    * Transactions with a custom timeout are not supported in Spark.
    *
    * @throws TransactionFailureException always
    */
  def execute(timeoutInSeconds: Int, runnable: TxRunnable): Unit

  /**
    * Creates a new instance of [[io.cdap.cdap.api.spark.dynamic.SparkInterpreter]] for Scala code compilation and
    * interpretation.
    *
    * @return a new instance of [[io.cdap.cdap.api.spark.dynamic.SparkInterpreter]]
    * @throws java.io.IOException if failed to create a local directory for storing the compiled class files
    */
  @throws(classOf[IOException])
  def createInterpreter(): SparkInterpreter

  /**
    * Get the information of the schedule that launches this Spark program, if there is any.
    *
    * @return a instance of [[scala.Some]] containing [[io.cdap.cdap.api.schedule.TriggeringScheduleInfo]]
    *         of the schedule that launches this Spark program. Return [[scala.None]] if the program is
    *         not launched by a schedule
    */
  def getTriggeringScheduleInfo: Option[TriggeringScheduleInfo]

  /**
    * Returns a new instance of [[io.cdap.cdap.api.spark.JavaSparkExecutionContext]].
    */
  def toJavaSparkExecutionContext(): JavaSparkExecutionContext
}
