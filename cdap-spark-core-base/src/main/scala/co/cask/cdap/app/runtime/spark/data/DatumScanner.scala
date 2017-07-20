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

import java.io.Closeable
import javax.annotation.Nullable

import co.cask.cdap.api.data.batch.{RecordScanner, SplitReader}
import co.cask.cdap.app.runtime.spark.data

/**
  * A Trait for unifying [[co.cask.cdap.api.data.batch.SplitReader]] and [[co.cask.cdap.api.data.batch.RecordScanner]].
  *
  * @tparam T type of the datum read
  */
trait DatumScanner[T] extends Closeable {

  /**
    * Reads in the next datum if there are more.
    *
    * @return `true` if successfully read the next datum; `false` otherwise
    */
  def next(): Boolean

  /**
    * Reads the last read datum after the `next` method was called
    *
    * @return the last read datum or `null` if nothing was read
    */
  @Nullable
  def getCurrent(): T
}

/**
  * Companion object for creating [[data.DatumScanner]].
  */
object DatumScanner {

  /**
    * Implicits conversion from [[co.cask.cdap.api.data.batch.SplitReader]]
    * to [[data.DatumScanner]].
    */
  implicit def fromSplitReader[K, V](reader: SplitReader[K, V]) : DatumScanner[(K, V)] = {
    new DatumScanner[(K, V)] {
      override def next(): Boolean = reader.nextKeyValue()
      override def getCurrent(): (K, V) = (reader.getCurrentKey, reader.getCurrentValue)
      override def close(): Unit = reader.close()
    }
  }

  /**
    * Implicits conversion from [[co.cask.cdap.api.data.batch.RecordScanner]]
    * to [[data.DatumScanner]].
    */
  implicit def fromRecordScanner[T](scanner: RecordScanner[T]) : DatumScanner[T] = {
    new DatumScanner[T] {
      override def next(): Boolean = scanner.nextRecord()
      override def getCurrent(): T = scanner.getCurrentRecord
      override def close(): Unit = scanner.close()
    }
  }
}
