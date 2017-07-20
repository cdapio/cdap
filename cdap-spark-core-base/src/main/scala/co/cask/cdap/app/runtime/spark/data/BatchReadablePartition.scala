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

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import co.cask.cdap.api.data.batch.Split
import com.google.gson.Gson
import org.apache.spark.Partition

/**
  * Represents one [[org.apache.spark.Partition]] in the [[co.cask.cdap.app.runtime.spark.data.BatchReadableRDD]], which
  * corresponds to one [[co.cask.cdap.api.data.batch.Split]].
  */
class BatchReadablePartition(private var _rddId: Int,
                             private var _index: Int,
                             private var _split: Split) extends Partition with Externalizable {

  /**
    * Default constructor. It is only for the deserialization
    */
  def this() = this(0, 0, null)

  /**
    * @return the [[co.cask.cdap.api.data.batch.Split]] contained inside this [[org.apache.spark.Partition]].
    */
  def split = _split

  override def index = _index

  override def writeExternal(out: ObjectOutput): Unit = {
    // Write the index, split class name and gson serialize the split
    out.writeInt(_rddId)
    out.writeInt(_index);
    out.writeUTF(_split.getClass.getName)
    out.writeUTF(new Gson().toJson(_split))
  }

  override def readExternal(in: ObjectInput): Unit = {
    // Read the index, split class name and gson deserialize the split
    _rddId = in.readInt()
    _index = in.readInt()
    var classLoader = Option(Thread.currentThread.getContextClassLoader).getOrElse(getClass.getClassLoader)
    val splitClass = classLoader.loadClass(in.readUTF()).asInstanceOf[Class[Split]]
    _split = new Gson().fromJson(in.readUTF(), splitClass)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[BatchReadablePartition]

  override def equals(other: Any): Boolean = {
    other match {
      case that: BatchReadablePartition =>
        (that canEqual this) &&
          _rddId == that._rddId &&
          _index == that._index &&
          _split == that._split
      case _ => false
    }
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), _rddId, _index, _split)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}