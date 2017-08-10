/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.spark.app

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.SparkExecutionContext
import co.cask.cdap.api.spark.SparkMain
import com.google.common.base.Splitter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.net.URI

import scala.collection.JavaConversions._

/**
  * Spark program that uses local files in Scala.
  */
class ScalaSparkUsingLocalFilesMain extends SparkMain {

  override def run(implicit sec: SparkExecutionContext) = {
    val sc = new SparkContext

    val args = sec.getRuntimeArguments
    val localFilePath = URI.create(args.get(SparkAppUsingLocalFiles.LOCAL_FILE_RUNTIME_ARG)).getPath
    val taskLocalizationContext = sec.getLocalizationContext
    val localFiles = taskLocalizationContext.getAllLocalFiles.values()

    localFiles.find(_.getName == SparkAppUsingLocalFiles.LOCAL_ARCHIVE_ALIAS).ensuring(_.isDefined)

    val fileContents: RDD[String] = sc.textFile(localFilePath, 1)
    fileContents
      .map {
        case line =>
          taskLocalizationContext.getLocalFile(SparkAppUsingLocalFiles.LOCAL_FILE_ALIAS).exists.==(true)
          taskLocalizationContext.getLocalFile(SparkAppUsingLocalFiles.LOCAL_ARCHIVE_ALIAS).exists.==(true)
          taskLocalizationContext.getLocalFile(SparkAppUsingLocalFiles.LOCAL_ARCHIVE_ALIAS).isDirectory.==(true)
          val splitter = Splitter.on("=").omitEmptyStrings().trimResults().split(line).iterator()
          (Bytes.toBytes(splitter.next), Bytes.toBytes(splitter.next))
      }
      .saveAsDataset(SparkAppUsingLocalFiles.OUTPUT_DATASET_NAME)
  }
}
