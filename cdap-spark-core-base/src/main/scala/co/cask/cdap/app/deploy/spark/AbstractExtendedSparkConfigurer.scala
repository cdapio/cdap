/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.deploy.spark

import co.cask.cdap.api.annotation.TransactionControl
import co.cask.cdap.api.service.http.ServiceHttpEndpoint
import co.cask.cdap.api.spark.ExtendedSparkConfigurer
import co.cask.cdap.api.spark.Spark
import co.cask.cdap.api.spark.SparkHttpServiceHandlerSpecification
import co.cask.cdap.api.spark.dynamic.SparkCompiler
import co.cask.cdap.api.spark.service.SparkHttpServiceHandler
import co.cask.cdap.app.runtime.spark.dynamic.AbstractSparkCompiler
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator
import co.cask.cdap.internal.app.runtime.service.http.HttpHandlerFactory
import co.cask.cdap.internal.app.services.ServiceEndpointExtractor
import co.cask.cdap.internal.app.spark.DefaultSparkConfigurer
import co.cask.cdap.internal.lang.Reflections
import co.cask.cdap.internal.specification.DataSetFieldExtractor
import co.cask.cdap.internal.specification.PropertyFieldExtractor
import co.cask.cdap.proto.Id

import java.lang
import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.tools.nsc.Settings

/**
  * Abstract class to provide common implementation for [[co.cask.cdap.api.spark.ExtendedSparkConfigurer]].
  */
abstract class AbstractExtendedSparkConfigurer(spark: Spark,
                                               deployNamespace: Id.Namespace,
                                               artifactId: Id.Artifact,
                                               artifactRepository: ArtifactRepository,
                                               pluginInstantiator: PluginInstantiator)
  extends DefaultSparkConfigurer(spark, deployNamespace, artifactId, artifactRepository, pluginInstantiator)
  with ExtendedSparkConfigurer {

  private val handlers = new mutable.ArrayBuffer[SparkHttpServiceHandler]()

  override def createSparkCompiler(): SparkCompiler = {
    return createSparkCompiler(AbstractSparkCompiler.setClassPath(new Settings))
  }

  def createSparkCompiler(settings: Settings): SparkCompiler

  /**
    * Adds a list of {@link SparkHttpServiceHandler}s to runs in the Spark driver.
    */
  override def addHandlers(handlers: lang.Iterable[_ <: SparkHttpServiceHandler]): Unit = {
    this.handlers ++= handlers
  }

  override protected def getHandlers: util.List[SparkHttpServiceHandlerSpecification] = {
    if (handlers.isEmpty) {
      return Seq()
    }

    // Validate the handlers and turn it into specification
    new HttpHandlerFactory("", TransactionControl.EXPLICIT).validateHttpHandler(handlers)
    handlers.map(handler => {
      val properties = new util.HashMap[String, String]()
      val datasets = new util.HashSet[String]()
      val endpoints = new util.ArrayList[ServiceHttpEndpoint]()

      Reflections.visit(handler, handler.getClass,
                        new PropertyFieldExtractor(properties), new DataSetFieldExtractor(datasets),
                        new ServiceEndpointExtractor(endpoints))
      new SparkHttpServiceHandlerSpecification(handler.getClass.getName, properties, datasets, endpoints)
    })
  }
}
