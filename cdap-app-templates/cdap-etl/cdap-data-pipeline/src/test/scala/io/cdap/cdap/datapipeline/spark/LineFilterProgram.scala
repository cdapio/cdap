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

package co.cask.cdap.datapipeline.spark

import co.cask.cdap.api.annotation.{Name, Plugin}
import co.cask.cdap.api.spark.{SparkExecutionContext, SparkMain}
import co.cask.cdap.datapipeline.ExternalSparkProgram
import co.cask.cdap.etl.common.Constants
import org.apache.spark.SparkContext

/**
 * A CDAP Spark program that filters out all lines that contain a configurable string
 */
@Plugin(`type` = Constants.SPARK_PROGRAM_PLUGIN_TYPE)
@Name("filterlines")
class LineFilterProgram(conf: CountConf) extends SparkMain {

  /**
   * This method will be called when the Spark program starts.
   *
   * @param sec the implicit context for interacting with CDAP
   */
  override def run(implicit sec: SparkExecutionContext): Unit = {
    val sc = new SparkContext()
    sec.getSpecification.getProperty(ExternalSparkProgram.STAGE_NAME)

    sc.textFile(conf.getInputPath).filter(line => !line.contains(conf.getFilterStr)).saveAsTextFile(conf.getOutputPath)
    sc.stop()
  }
}
