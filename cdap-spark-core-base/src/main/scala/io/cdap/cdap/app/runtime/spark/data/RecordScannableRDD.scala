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

package io.cdap.cdap.app.runtime.spark.data

import io.cdap.cdap.api.data.batch.RecordScannable
import io.cdap.cdap.api.data.batch.Split
import io.cdap.cdap.api.dataset.Dataset
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import java.net.URI

import scala.annotation.meta.param
import scala.reflect.ClassTag

/**
  * A [[org.apache.spark.rdd.RDD]] implementation that reads data
  * through [[io.cdap.cdap.api.data.batch.RecordScannable]].
  */

class RecordScannableRDD[R: ClassTag](@(transient @param) sc: SparkContext,
                                      namespace: String,
                                      datasetName: String,
                                      arguments: Map[String, String],
                                      @(transient @param) splits: Iterable[_ <: Split],
                                      txServiceBaseURI: Broadcast[URI])
  extends DatumScannerBasedRDD[R](sc, namespace, datasetName, arguments, splits, txServiceBaseURI) {

  override protected def createDatumScanner(dataset: Dataset, split: Split): DatumScanner[R] = {
    val splitReader = dataset.asInstanceOf[RecordScannable[R]].createSplitRecordScanner(split)
    splitReader.initialize(split)
    splitReader
  }
}
