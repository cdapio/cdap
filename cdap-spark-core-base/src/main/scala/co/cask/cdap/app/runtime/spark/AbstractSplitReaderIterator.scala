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

import java.util.concurrent.atomic.AtomicBoolean

import co.cask.cdap.api.data.batch.SplitReader
import org.apache.spark.{TaskContext, TaskKilledException}
import org.apache.spark.executor.InputMetrics


/**
  * An [[scala.Iterator]] that is backed by a [[co.cask.cdap.api.data.batch.SplitReader]].
  */
abstract class AbstractSplitReaderIterator[K, V](context: TaskContext,
                                                 splitReader: SplitReader[K, V],
                                                 closeable: () => Unit) extends Iterator[(K, V)] {
  val done = new AtomicBoolean
  var nextKeyValue: Option[(K, V)] = None

  override def hasNext: Boolean = {
    if (done.get()) {
      return false
    }
    if (context.isInterrupted()) {
      throw new TaskKilledException
    }

    // Find the next key value if necessary
    if (nextKeyValue.isEmpty && splitReader.nextKeyValue()) {
      nextKeyValue = Some((splitReader.getCurrentKey, splitReader.getCurrentValue))
    }
    return nextKeyValue.isDefined
  }

  override def next: (K, V) = {
    // Precondition check. It shouldn't fail.
    require(nextKeyValue.isDefined || hasNext, "No more entries")

    incrementMetrics()
    val result = nextKeyValue.get
    nextKeyValue = None
    result
  }

  def close: Unit = {
    if (done.compareAndSet(false, true)) {
      closeable()
    }
  }

  def incrementMetrics(): Unit
}
