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

import co.cask.cdap.app.runtime.spark.data
import org.apache.spark.TaskContext
import org.apache.spark.TaskKilledException

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean

/**
  * An adapter class to branch a [[data.DatumScanner]] into an [[scala.Iterator]].
  */
abstract class AbstractDatumScannerIterator[T](context: TaskContext,
                                               scanner: DatumScanner[T]) extends Iterator[T] with Closeable {

  val done = new AtomicBoolean
  var nextKeyValue: Option[T] = None

  override def hasNext: Boolean = {
    if (done.get()) {
      return false
    }
    if (context.isInterrupted()) {
      throw new TaskKilledException
    }

    // Find the next key value if necessary
    if (nextKeyValue.isEmpty && scanner.next()) {
      nextKeyValue = Some(scanner.getCurrent())
    }
    return nextKeyValue.isDefined
  }

  override def next: T = {
    // Precondition check. It shouldn't fail.
    if (!nextKeyValue.isDefined && !hasNext) {
      throw new NoSuchElementException("No more entries")
    }

    incrementMetrics()
    val result = nextKeyValue.get
    nextKeyValue = None
    result
  }

  override def close: Unit = {
    if (done.compareAndSet(false, true)) {
      scanner.close()
    }
  }

  /**
    * Increments the metrics for reading a record. By default is a no-op.
    */
  def incrementMetrics(): Unit = {

  }
}
