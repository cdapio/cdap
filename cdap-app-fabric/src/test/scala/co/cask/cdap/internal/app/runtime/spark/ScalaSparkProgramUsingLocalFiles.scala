/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark

import java.io.File
import java.net.URI
import java.util

import co.cask.cdap.api.TaskLocalizationContext
import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.{ScalaSparkProgram, SparkContext}
import com.google.common.base.Splitter
import org.apache.spark
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions

/**
 * Spark program that uses local files in Scala.
 */
class ScalaSparkProgramUsingLocalFiles extends ScalaSparkProgram {
  override def run(context: SparkContext): Unit = {
    val args: util.Map[String, String] = context.getRuntimeArguments
    val localFilePath: String = URI.create(args.get(SparkAppUsingLocalFiles.LOCAL_FILE_RUNTIME_ARG)).getPath
    val taskLocalizationContext: TaskLocalizationContext = context.getTaskLocalizationContext
    val localFiles: util.Collection[File] = taskLocalizationContext.getAllLocalFiles.values()
    JavaConversions.collectionAsScalaIterable(localFiles)
      .find(_.getName == SparkAppUsingLocalFiles.LOCAL_FILE_ALIAS).get
    val sc: spark.SparkContext = context.getOriginalSparkContext.asInstanceOf[spark.SparkContext]
    val fileContents: RDD[String] = sc.textFile(localFilePath, 1)
    val rows: RDD[(Array[Byte], Array[Byte])] = fileContents.map {
      case line =>
        taskLocalizationContext.getLocalFile(SparkAppUsingLocalFiles.LOCAL_FILE_ALIAS).exists.==(true)
        taskLocalizationContext.getLocalFile(SparkAppUsingLocalFiles.LOCAL_ARCHIVE_ALIAS).exists.==(true)
        taskLocalizationContext.getLocalFile(SparkAppUsingLocalFiles.LOCAL_ARCHIVE_ALIAS).isDirectory.==(true)
        val splitter: util.Iterator[String] = Splitter.on("=").omitEmptyStrings().trimResults().split(line).iterator()
        val key: String = splitter.next()
        val value: String = splitter.next()
        (Bytes.toBytes(key), Bytes.toBytes(value))
    }
    context.writeToDataset(
      rows, SparkAppUsingLocalFiles.OUTPUT_DATASET_NAME, classOf[Array[Byte]], classOf[Array[Byte]])
  }
}
