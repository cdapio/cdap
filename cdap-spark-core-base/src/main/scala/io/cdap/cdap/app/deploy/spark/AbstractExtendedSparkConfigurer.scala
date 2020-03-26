/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package io.cdap.cdap.app.deploy.spark

import io.cdap.cdap.api.annotation.TransactionControl
import io.cdap.cdap.api.service.http.ServiceHttpEndpoint
import io.cdap.cdap.api.spark.ExtendedSparkConfigurer
import io.cdap.cdap.api.spark.Spark
import io.cdap.cdap.api.spark.SparkHttpServiceHandlerSpecification
import io.cdap.cdap.api.spark.dynamic.SparkCompiler
import io.cdap.cdap.api.spark.service.SparkHttpServiceHandler
import io.cdap.cdap.app.runtime.spark.dynamic.AbstractSparkCompiler
import io.cdap.cdap.common.id.Id
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator
import io.cdap.cdap.internal.app.runtime.service.http.HttpHandlerFactory
import io.cdap.cdap.internal.app.services.ServiceEndpointExtractor
import io.cdap.cdap.internal.app.spark.DefaultSparkConfigurer
import io.cdap.cdap.internal.lang.Reflections
import io.cdap.cdap.internal.specification.DataSetFieldExtractor
import io.cdap.cdap.internal.specification.PropertyFieldExtractor

import java.lang
import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.tools.nsc.Settings

/**
  * Abstract class to provide common implementation for [[io.cdap.cdap.api.spark.ExtendedSparkConfigurer]].
  */
abstract class AbstractExtendedSparkConfigurer(spark: Spark,
                                               deployNamespace: Id.Namespace,
                                               artifactId: Id.Artifact,
                                               pluginFinder: PluginFinder,
                                               pluginInstantiator: PluginInstantiator)
  extends DefaultSparkConfigurer(spark, deployNamespace, artifactId, pluginFinder, pluginInstantiator)
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
