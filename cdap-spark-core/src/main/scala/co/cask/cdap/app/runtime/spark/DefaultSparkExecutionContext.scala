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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Spark1 SparkExecutionContext
  */
class DefaultSparkExecutionContext(runtimeContext: SparkRuntimeContext, localizeResources: util.Map[String, File])
  extends AbstractSparkExecutionContext(runtimeContext, localizeResources) {

  override protected def saveAsNewAPIHadoopDataset[K: ClassManifest, V: ClassManifest](sc: SparkContext,
                                                                                       conf: Configuration,
                                                                                       rdd: RDD[(K, V)]): Unit = {
    // In Spark 1.2, we have to use the SparkContext.rddToPairRDDFunctions because the implicit
    // conversion from RDD is not available.
    if (sc.version == "1.2" || sc.version.startsWith("1.2.")) {
      SparkContext.rddToPairRDDFunctions(rdd).saveAsNewAPIHadoopDataset(conf)
    } else {
      rdd.saveAsNewAPIHadoopDataset(conf)
    }
  }
}
