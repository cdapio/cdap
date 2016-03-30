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

package co.cask.cdap.app.runtime.spark

import java.lang.Iterable
import java.util

import co.cask.cdap.api.app.ApplicationSpecification
import co.cask.cdap.api.data.batch.Split
import co.cask.cdap.api.flow.flowlet.StreamEvent
import co.cask.cdap.api.metrics.Metrics
import co.cask.cdap.api.plugin.PluginContext
import co.cask.cdap.api.spark.{JavaSparkExecutionContext, SparkExecutionContext, SparkSpecification}
import co.cask.cdap.api.workflow.WorkflowToken
import co.cask.cdap.api.{Admin, ServiceDiscoverer, TxRunnable}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.twill.api.RunId

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * Implementation of [[co.cask.cdap.api.spark.JavaSparkExecutionContext]] that simply delegates all calls to
  * a [[co.cask.cdap.api.spark.SparkExecutionContext]].
  */
class DefaultJavaSparkExecutionContext(sec: SparkExecutionContext) extends JavaSparkExecutionContext {

  override def getSpecification: SparkSpecification = sec.getSpecification

  override def getMetrics: Metrics = sec.getMetrics

  override def getServiceDiscoverer: ServiceDiscoverer = sec.getServiceDiscoverer

  override def getLogicalStartTime: Long = sec.getLogicalStartTime

  override def getPluginContext: PluginContext = sec.getPluginContext

  override def getWorkflowToken: WorkflowToken = sec.getWorkflowToken.orNull

  override def getRuntimeArguments: util.Map[String, String] = sec.getRuntimeArguments

  override def getRunId: RunId = sec.getRunId

  override def getNamespace: String = sec.getNamespace

  override def getApplicationSpecification: ApplicationSpecification = sec.getApplicationSpecification

  override def getAdmin: Admin = sec.getAdmin

  override def execute(runnable: TxRunnable): Unit = sec.execute(runnable)

  override def fromDataset[K, V](datasetName: String, arguments: util.Map[String, String],
                                 splits: Iterable[_ <: Split]): JavaPairRDD[K, V] = {
    // Create the implicit fake ClassTags to satisfy scala type system at compilation time.
    implicit val kTag: ClassTag[K] = createClassTag
    implicit val vTag: ClassTag[V] = createClassTag
    JavaPairRDD.fromRDD(sec.fromDataset(SparkContextCache.getContext, datasetName, arguments.toMap, Some(splits)))
  }

  override def fromStream(streamName: String, startTime: Long, endTime: Long) : JavaRDD[StreamEvent] = {
    val ct: ClassTag[StreamEvent] = createClassTag
    JavaRDD.fromRDD(
      sec.fromStream(SparkContextCache.getContext, streamName, startTime, endTime)(ct, (e: StreamEvent) => e))
  }

  override def saveAsDataset[K, V](rdd: JavaPairRDD[K, V], datasetName: String,
                                   arguments: util.Map[String, String]): Unit = {
    // Create the implicit fake ClassTags to satisfy scala type system at compilation time.
    implicit val kTag: ClassTag[K] = createClassTag
    implicit val vTag: ClassTag[V] = createClassTag
    sec.saveAsDataset(JavaPairRDD.toRDD(rdd), datasetName, arguments.toMap)
  }

  /**
    * Creates a [[scala.reflect.ClassTag]] for the parameterized type T.
    */
  private def createClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]
}
