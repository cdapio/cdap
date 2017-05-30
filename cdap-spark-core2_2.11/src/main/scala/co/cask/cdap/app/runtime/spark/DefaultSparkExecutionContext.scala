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

package co.cask.cdap.app.runtime.spark
import java.io.File
import java.util

import co.cask.cdap.api.spark.dynamic.SparkInterpreter
import co.cask.cdap.app.runtime.spark.dynamic.DefaultSparkInterpreter
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.tools.nsc.Settings

/**
  * Spark2 SparkExecutionContext
  */
class DefaultSparkExecutionContext(sparkClassLoader: SparkClassLoader, localizeResources: util.Map[String, File])
  extends AbstractSparkExecutionContext(sparkClassLoader, localizeResources) {

  override protected def saveAsNewAPIHadoopDataset[K: ClassManifest, V: ClassManifest](sc: SparkContext,
                                                                                       conf: Configuration,
                                                                                       rdd: RDD[(K, V)]): Unit = {
    rdd.saveAsNewAPIHadoopDataset(conf)
  }

  override protected def createInterpreter(settings: Settings,
                                           classDir: File, onClose: () => Unit): SparkInterpreter = {
    settings.Yreploutdir.value = classDir.getAbsolutePath
    new DefaultSparkInterpreter(settings, onClose)
  }
}
