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

import co.cask.cdap.api.data.batch.SplitReader
import org.apache.spark.TaskContext
import org.apache.spark.executor.InputMetrics

/**
  * Spark1 SplitReaderIterator, which has access to Spark's metrics.
  */
class SplitReaderIterator[K, V](context: TaskContext, splitReader: SplitReader[K, V], closeable: () => Unit)
  extends AbstractSplitReaderIterator(context, splitReader, closeable) {

  val inputMetrics: Option[InputMetrics] = context.taskMetrics.inputMetrics

  override def incrementMetrics(): Unit = {
    // TODO: Add metrics for size being read. It requires dataset to expose it.
    inputMetrics.foreach(_.incRecordsRead(1L))
  }
}
