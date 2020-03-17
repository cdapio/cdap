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

package io.cdap.cdap.app.runtime.spark

import java.io.IOException
import java.lang
import java.util

import io.cdap.cdap.api.Admin
import io.cdap.cdap.api.ServiceDiscoverer
import io.cdap.cdap.api.TxRunnable
import io.cdap.cdap.api.app.ApplicationSpecification
import io.cdap.cdap.api.data.batch.Split
import io.cdap.cdap.api.lineage.field.Operation
import io.cdap.cdap.api.messaging.MessagingContext
import io.cdap.cdap.api.metadata.Metadata
import io.cdap.cdap.api.metadata.MetadataEntity
import io.cdap.cdap.api.metadata.MetadataScope
import io.cdap.cdap.api.metrics.Metrics
import io.cdap.cdap.api.plugin.PluginContext
import io.cdap.cdap.api.preview.DataTracer
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo
import io.cdap.cdap.api.security.store.{SecureStore, SecureStoreData, SecureStoreMetadata}
import io.cdap.cdap.api.spark.JavaSparkExecutionContext
import io.cdap.cdap.api.spark.SparkExecutionContext
import io.cdap.cdap.api.spark.SparkSpecification
import io.cdap.cdap.api.spark.dynamic.SparkInterpreter
import io.cdap.cdap.api.workflow.WorkflowInfo
import io.cdap.cdap.api.workflow.WorkflowToken
import org.apache.spark.api.java.JavaPairRDD
import org.apache.twill.api.RunId

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * Implementation of [[io.cdap.cdap.api.spark.JavaSparkExecutionContext]] that simply delegates all calls to
  * a [[io.cdap.cdap.api.spark.SparkExecutionContext]].
  */
@SerialVersionUID(0L)
class DefaultJavaSparkExecutionContext(sec: SparkExecutionContext) extends JavaSparkExecutionContext with Serializable {

  override def getSpecification: SparkSpecification = sec.getSpecification

  override def getMetrics: Metrics = sec.getMetrics

  override def getServiceDiscoverer: ServiceDiscoverer = sec.getServiceDiscoverer

  override def getLogicalStartTime: Long = sec.getLogicalStartTime

  override def getPluginContext: PluginContext = sec.getPluginContext

  override def getSecureStore: SecureStore = sec.getSecureStore

  override def getMessagingContext: MessagingContext = sec.getMessagingContext

  override def getWorkflowToken: WorkflowToken = sec.getWorkflowToken.orNull

  override def getWorkflowInfo: WorkflowInfo = sec.getWorkflowInfo.orNull

  override def getLocalizationContext = sec.getLocalizationContext

  override def getClusterName: String = sec.getClusterName

  override def getRuntimeArguments: util.Map[String, String] = sec.getRuntimeArguments

  override def getRunId: RunId = sec.getRunId

  override def getNamespace: String = sec.getNamespace

  override def getApplicationSpecification: ApplicationSpecification = sec.getApplicationSpecification

  override def getAdmin: Admin = sec.getAdmin

  override def execute(runnable: TxRunnable): Unit = sec.execute(runnable)

  override def execute(timeout: Int, runnable: TxRunnable): Unit = sec.execute(timeout, runnable)

  override def createInterpreter(): SparkInterpreter = sec.createInterpreter()

  override def getSparkExecutionContext: SparkExecutionContext = sec

  override def fromDataset[K, V](datasetName: String, arguments: util.Map[String, String],
                                 splits: java.lang.Iterable[_ <: Split]): JavaPairRDD[K, V] = {
    // Create the implicit fake ClassTags to satisfy scala type system at compilation time.
    implicit val kTag: ClassTag[K] = createClassTag
    implicit val vTag: ClassTag[V] = createClassTag
    JavaPairRDD.fromRDD(
      sec.fromDataset(SparkRuntimeEnv.getContext, datasetName, arguments.toMap, Option(splits).map(_.toIterable)))
  }

  override def fromDataset[K, V](namespace: String, datasetName: String, arguments: util.Map[String, String],
                                 splits: java.lang.Iterable[_ <: Split]): JavaPairRDD[K, V] = {
    // Create the implicit fake ClassTags to satisfy scala type system at compilation time.
    implicit val kTag: ClassTag[K] = createClassTag
    implicit val vTag: ClassTag[V] = createClassTag
    JavaPairRDD.fromRDD(
      sec.fromDataset(SparkRuntimeEnv.getContext, namespace, datasetName, arguments.toMap,
        Option(splits).map(_.toIterable)))
  }

  override def saveAsDataset[K, V](rdd: JavaPairRDD[K, V], datasetName: String,
                                   arguments: util.Map[String, String]): Unit = {
    saveAsDataset(rdd, getNamespace, datasetName, arguments)
  }

  override def saveAsDataset[K, V](rdd: JavaPairRDD[K, V], namespace: String, datasetName: String,
                                   arguments: util.Map[String, String]): Unit = {
    // Create the implicit fake ClassTags to satisfy scala type system at compilation time.
    implicit val kTag: ClassTag[K] = createClassTag
    implicit val vTag: ClassTag[V] = createClassTag
    sec.saveAsDataset(JavaPairRDD.toRDD(rdd), namespace, datasetName, arguments.toMap)
  }

  override def getDataTracer(loggerName: String): DataTracer = sec.getDataTracer(loggerName)

  override def getTriggeringScheduleInfo: TriggeringScheduleInfo = sec.getTriggeringScheduleInfo.getOrElse(null)

  @throws[IOException]
  override def list(namespace: String): util.List[SecureStoreMetadata] = {
    return sec.getSecureStore.list(namespace)
  }

  @throws[IOException]
  override def get(namespace: String, name: String): SecureStoreData = {
    return sec.getSecureStore.get(namespace, name)
  }

  /**
    * Creates a [[scala.reflect.ClassTag]] for the parameterized type T.
    */
  private def createClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

  override def getMetadata(metadataEntity: MetadataEntity): util.Map[MetadataScope, Metadata] = {
    return sec.getMetadata(metadataEntity);
  }

  override def getMetadata(scope: MetadataScope, metadataEntity: MetadataEntity): Metadata = {
    return sec.getMetadata(scope, metadataEntity);
  }

  override def addProperties(metadataEntity: MetadataEntity, properties: util.Map[String, String]) = {
    sec.addProperties(metadataEntity, properties);
  }

  override def addTags(metadataEntity: MetadataEntity, tags: String*) = {
    sec.addTags(metadataEntity, tags)
  }

  override def addTags(metadataEntity: MetadataEntity, tags: lang.Iterable[String]): Unit = {
    sec.addTags(metadataEntity, tags)
  }

  override def removeMetadata(metadataEntity: MetadataEntity): Unit = {
    sec.removeMetadata(metadataEntity)
  }

  override def removeProperties(metadataEntity: MetadataEntity): Unit = {
    sec.removeProperties(metadataEntity)
  }

  override def removeProperties(metadataEntity: MetadataEntity, keys: String*): Unit = {
    sec.removeProperties(metadataEntity, keys:_*)
  }

  override def removeTags(metadataEntity: MetadataEntity): Unit = {
    sec.removeTags(metadataEntity)
  }

  override def removeTags(metadataEntity: MetadataEntity, tags: String*): Unit = {
    sec.removeTags(metadataEntity, tags:_*)
  }

  override def record(operations: util.Collection[_ <: Operation]): Unit = {
    sec.record(operations)
  }

  override def flushLineage(): Unit = {
    sec.flushLineage()
  }
}
