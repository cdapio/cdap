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

package co.cask.cdap.app.runtime.spark.data

import java.net.URI

import co.cask.cdap.api.data.batch.{BatchReadable, Split}
import co.cask.cdap.api.dataset.Dataset
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.annotation.meta.param
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * A [[org.apache.spark.rdd.RDD]] implementation that reads data through [[co.cask.cdap.api.data.batch.BatchReadable]].
  */
class BatchReadableRDD[K: ClassTag, V: ClassTag](@(transient @param) sc: SparkContext,
                                                 @(transient @param) batchReadable: BatchReadable[K, V],
                                                 namespace: String,
                                                 datasetName: String,
                                                 arguments: Map[String, String],
                                                 @(transient @param) splits: Iterable[_ <: Split],
                                                 txServiceBaseURI: Broadcast[URI])
  extends DatumScannerBasedRDD[(K, V)](sc, namespace, datasetName, arguments, splits, txServiceBaseURI) {

  override protected def createDatumScanner(dataset: Dataset, split: Split): DatumScanner[(K, V)] = {
    val splitReader = dataset.asInstanceOf[BatchReadable[K, V]].createSplitReader(split)
    splitReader.initialize(split)
    splitReader
  }
}
