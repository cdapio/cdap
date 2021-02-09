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

package io.cdap.cdap.app.runtime.spark

import java.io.File
import java.io.IOException
import java.util

import io.cdap.cdap.api.spark.dynamic.SparkInterpreter
import io.cdap.cdap.app.runtime.spark.dynamic.DefaultSparkInterpreter
import io.cdap.cdap.app.runtime.spark.dynamic.URLAdder
import io.cdap.cdap.common.conf.Constants
import io.cdap.cdap.common.utils.DirUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.tools.nsc.Settings

/**
  * Spark2 SparkExecutionContext
  */
class DefaultSparkExecutionContext(sparkClassLoader: SparkClassLoader, localizeResources: util.Map[String, File])
  extends AbstractSparkExecutionContext(sparkClassLoader, localizeResources) {

  // Import the companion object for static fields
  import DefaultSparkExecutionContext._

  private val classOutputDir =
    new File(new File(runtimeContext.getCConfiguration.get(Constants.CFG_LOCAL_DATA_DIR),
      runtimeContext.getCConfiguration.get(Constants.AppFabric.TEMP_DIR)),
      runtimeContext.getRunId.getId).getAbsoluteFile

  classOutputDir.mkdirs()
  SparkRuntimeEnv.setProperty("spark.repl.class.outputDir", classOutputDir.getAbsolutePath);

  override protected def saveAsNewAPIHadoopDataset[K: ClassManifest, V: ClassManifest](sc: SparkContext,
                                                                                       conf: Configuration,
                                                                                       rdd: RDD[(K, V)]): Unit = {
    // Spark expects the conf to be the job configuration, and to contain the credentials
    val jobConf = new JobConf(conf)
    jobConf.setCredentials(UserGroupInformation.getCurrentUser.getCredentials)
    rdd.saveAsNewAPIHadoopDataset(jobConf)
  }

  override protected def createInterpreter(settings: Settings, classDir: File,
                                           urlAdder: URLAdder, onClose: () => Unit): SparkInterpreter = {
    settings.Yreploutdir.value = classDir.getAbsolutePath
    new DefaultSparkInterpreter(settings, urlAdder, () => {
      onClose()
      DirUtils.deleteDirectoryContents(classDir, true)
    })
  }

  override protected def createInterpreterOutputDir(interpreterCount: Int): File = classOutputDir

  override def close(): Unit = {
    try {
      super.close()
    } finally {
      try {
        DirUtils.deleteDirectoryContents(classOutputDir)
      } catch {
        case t: IOException => LOG.warn("Failed to delete directory {}", Array[AnyRef](classOutputDir, t): _*)
      }
    }
  }
}

object DefaultSparkExecutionContext {
  private val LOG = LoggerFactory.getLogger(classOf[AbstractSparkExecutionContext])
}
