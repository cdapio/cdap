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

package co.cask.cdap.app.runtime.spark

import java.net.URI
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import co.cask.cdap.api.data.batch.{BatchReadable, Split, SplitReader}
import co.cask.cdap.api.dataset.Dataset
import co.cask.cdap.data2.metadata.lineage.AccessType
import co.cask.tephra.TransactionAware
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.InputMetrics
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * A [[org.apache.spark.rdd.RDD]] implementation that reads data through [[co.cask.cdap.api.data.batch.BatchReadable]].
  */
class BatchReadableRDD[K: ClassTag, V: ClassTag](@transient sc: SparkContext,
                                                 @transient batchReadable: BatchReadable[K, V],
                                                 datasetName: String,
                                                 arguments: Map[String, String],
                                                 @transient splits: Option[Iterable[_ <: Split]],
                                                 txServiceBaseURI: Broadcast[URI]) extends RDD[(K, V)](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    val inputSplits: Iterable[_ <: Split] = splits.getOrElse(batchReadable.getSplits.toIterable)
    inputSplits.zipWithIndex.map(t => new BatchReadablePartition(id, t._2, t._1)).toArray
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[(K, V)] = {
    val inputMetrics = context.taskMetrics.inputMetrics
    val split = partition.asInstanceOf[BatchReadablePartition].split
    val sparkTxClient = new SparkTransactionClient(txServiceBaseURI.value)

    val datasetCache = SparkRuntimeContextProvider.get().getDatasetCache
    val dataset: Dataset = datasetCache.getDataset(datasetName, arguments, true, AccessType.READ)

    try {
      // Get the Transaction of the dataset if it is TransactionAware
      dataset match {
        case txAware: TransactionAware => {
          // Try to get the transaction for this stage. Hardcoded the timeout to 10 seconds for now
          txAware.startTx(sparkTxClient.getTransaction(context.stageId(), 10, TimeUnit.SECONDS))
        }
        case _ => // Nothing happen
      }

      // Creates the split reader and use it to construct the Iterator to result
      val splitReader = dataset.asInstanceOf[BatchReadable[K, V]].createSplitReader(split)
      splitReader.initialize(split)
      val iterator = new SplitReaderIterator[K, V](context, splitReader, inputMetrics, () => {
        try {
          splitReader.close()
        } finally {
          dataset.close()
        }
      })

      context.addTaskCompletionListener(context => iterator.close)
      iterator
    } catch {
      case t: Throwable =>
        dataset.close()
        throw t
    }
  }

  /**
    * An [[scala.Iterator]] that is backed by a [[co.cask.cdap.api.data.batch.SplitReader]].
    */
  private class SplitReaderIterator[K, V](context: TaskContext,
                                          splitReader: SplitReader[K, V],
                                          inputMetrics: Option[InputMetrics],
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

      // TODO: Add metrics for size being read. It requires dataset to expose it.
      inputMetrics.foreach(_.incRecordsRead(1L))
      val result = nextKeyValue.get
      nextKeyValue = None
      result
    }

    def close: Unit = {
      if (done.compareAndSet(false, true)) {
        closeable()
      }
    }
  }
}
