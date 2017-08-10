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

import co.cask.cdap.api.spark.ExtendedSparkConfigurer
import co.cask.cdap.api.spark.Spark
import co.cask.cdap.api.spark.dynamic.SparkCompiler
import co.cask.cdap.app.runtime.spark.dynamic.AbstractSparkCompiler
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator
import co.cask.cdap.internal.app.spark.DefaultSparkConfigurer
import co.cask.cdap.proto.Id

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

  override def createSparkCompiler(): SparkCompiler = {
    return createSparkCompiler(AbstractSparkCompiler.setClassPath(new Settings))
  }

  def createSparkCompiler(settings: Settings): SparkCompiler
}
