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

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.{lang, util}

import io.cdap.cdap.api.TxRunnable
import io.cdap.cdap.api.data.batch.Split
import io.cdap.cdap.api.lineage.field.Operation
import io.cdap.cdap.api.metadata.{Metadata, MetadataEntity, MetadataScope}
import io.cdap.cdap.api.preview.DataTracer
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo
import io.cdap.cdap.api.spark.SparkExecutionContext
import io.cdap.cdap.api.spark.dynamic.SparkInterpreter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * A [[io.cdap.cdap.api.spark.SparkExecutionContext]] that is Serializable. It delegates all operations to
  * the [[io.cdap.cdap.api.spark.SparkExecutionContext]] in the runtime context.
  */
class SerializableSparkExecutionContext(val delegate: SparkExecutionContext) extends SparkExecutionContext
                                                                             with Externalizable {

  /**
    * Default constructor for deserialization
    */
  def this() = {
    this(SparkClassLoader.findFromContext().getSparkExecutionContext(false))
  }

  override def getAdmin = delegate.getAdmin

  override def getRunId = delegate.getRunId

  override def getNamespace = delegate.getNamespace

  override def getRuntimeArguments = delegate.getRuntimeArguments

  override def getClusterName = delegate.getClusterName

  override def getApplicationSpecification = delegate.getApplicationSpecification

  override def execute(runnable: TxRunnable) = delegate.execute(runnable)

  override def execute(timeout: Int, runnable: TxRunnable) = delegate.execute(timeout, runnable)

  override def createInterpreter(): SparkInterpreter = delegate.createInterpreter()

  override def saveAsDataset[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], namespace: String,
                                                       datasetName: String, arguments: Map[String, String]) =
    delegate.saveAsDataset(rdd, namespace, datasetName, arguments)

  override def saveAsDataset[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], datasetName: String,
                                                       arguments: Map[String, String]) =
    delegate.saveAsDataset(rdd, datasetName, arguments)

  override def fromDataset[K: ClassTag, V: ClassTag](sc: SparkContext, namespace: String,
                                                     datasetName: String, arguments: Map[String, String],
                                                     splits: Option[Iterable[_ <: Split]]) =
    delegate.fromDataset[K, V](sc, namespace, datasetName, arguments, splits)

  override def fromDataset[K: ClassTag, V: ClassTag](sc: SparkContext, datasetName: String,
                                                     arguments: Map[String, String],
                                                     splits: Option[Iterable[_ <: Split]]) =
    delegate.fromDataset[K, V](sc, datasetName, arguments, splits)

  override def getLocalizationContext = delegate.getLocalizationContext

  override def getWorkflowInfo = delegate.getWorkflowInfo

  override def getWorkflowToken = delegate.getWorkflowToken

  override def getSecureStore = delegate.getSecureStore

  override def getMessagingContext = delegate.getMessagingContext

  override def getPluginContext = delegate.getPluginContext

  override def getMetrics = delegate.getMetrics

  override def getServiceDiscoverer = delegate.getServiceDiscoverer

  override def getLogicalStartTime = delegate.getLogicalStartTime

  override def getSpecification = delegate.getSpecification

  override def readExternal(in: ObjectInput) = {
    // no-op
  }

  override def writeExternal(out: ObjectOutput) = {
    // no-op
  }

  override def getMetadata(metadataEntity: MetadataEntity): util.Map[MetadataScope, Metadata] = {
    return delegate.getMetadata(metadataEntity);
  }

  override def getMetadata(scope: MetadataScope, metadataEntity: MetadataEntity): Metadata = {
    return delegate.getMetadata(scope, metadataEntity);
  }

  override def addProperties(metadataEntity: MetadataEntity, properties: util.Map[String, String]) = {
    delegate.addProperties(metadataEntity, properties);
  }

  override def addTags(metadataEntity: MetadataEntity, tags: String*) = {
    delegate.addTags(metadataEntity, tags:_*)
  }

  override def addTags(metadataEntity: MetadataEntity, tags: lang.Iterable[String]): Unit = {
    delegate.addTags(metadataEntity, tags)
  }

  override def removeMetadata(metadataEntity: MetadataEntity): Unit = {
    delegate.removeMetadata(metadataEntity)
  }

  override def removeProperties(metadataEntity: MetadataEntity): Unit = {
    delegate.removeProperties(metadataEntity)
  }

  override def removeProperties(metadataEntity: MetadataEntity, keys: String*): Unit = {
    delegate.removeProperties(metadataEntity, keys:_*)
  }

  override def removeTags(metadataEntity: MetadataEntity): Unit = {
    delegate.removeTags(metadataEntity)
  }

  override def removeTags(metadataEntity: MetadataEntity, tags: String*): Unit = {
    delegate.removeTags(metadataEntity, tags:_*)
  }

  override def getDataTracer(loggerName: String): DataTracer = delegate.getDataTracer(loggerName)

  override def getTriggeringScheduleInfo: Option[TriggeringScheduleInfo] = delegate.getTriggeringScheduleInfo

  override def toJavaSparkExecutionContext() = delegate.toJavaSparkExecutionContext()

  override def record(operations: util.Collection[_ <: Operation]): Unit = {
    delegate.record(operations)
  }

  override def flushLineage(): Unit = {
    delegate.flushLineage()
  }
}
