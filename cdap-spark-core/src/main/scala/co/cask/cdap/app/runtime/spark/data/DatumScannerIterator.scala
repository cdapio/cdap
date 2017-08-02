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

import org.apache.spark.TaskContext
import org.apache.spark.executor.InputMetrics

/**
  * Spark 1 implementation of [[co.cask.cdap.app.runtime.spark.data.AbstractDatumScannerIterator]] that supports
  * emitting metrics.
  */
class DatumScannerIterator[T](context: TaskContext,
                              scanner: DatumScanner[T]) extends AbstractDatumScannerIterator[T](context, scanner) {

  val inputMetrics: Option[InputMetrics] = context.taskMetrics.inputMetrics

  override def incrementMetrics(): Unit = {
    // TODO: Add metrics for size being read. It requires dataset to expose it.
    inputMetrics.foreach(_.incRecordsRead(1L))
  }
}
