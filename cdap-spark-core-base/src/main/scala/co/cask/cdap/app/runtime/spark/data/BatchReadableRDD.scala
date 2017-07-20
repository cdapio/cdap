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
import java.util.concurrent.TimeUnit

import co.cask.cdap.api.data.batch.{BatchReadable, Split}
import co.cask.cdap.api.dataset.Dataset
import co.cask.cdap.app.runtime.spark.{SparkRuntimeContextProvider, SparkTransactionClient}
import co.cask.cdap.data2.metadata.lineage.AccessType
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.tephra.TransactionAware

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
                                                 @(transient @param) splits: Option[Iterable[_ <: Split]],
                                                 txServiceBaseURI: Broadcast[URI]) extends RDD[(K, V)](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    val inputSplits: Iterable[_ <: Split] = splits.getOrElse(batchReadable.getSplits.toIterable)
    inputSplits.zipWithIndex.map(t => new BatchReadablePartition(id, t._2, t._1)).toArray
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[(K, V)] = {
    val split = partition.asInstanceOf[BatchReadablePartition].split
    val sparkTxClient = new SparkTransactionClient(txServiceBaseURI.value)

    val datasetCache = SparkRuntimeContextProvider.get().getDatasetCache

    val dataset: Dataset = datasetCache.getDataset(namespace, datasetName, arguments, true, AccessType.READ)

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

      val iterator = new SplitReaderIterator[K, V](context, splitReader)
      context.addTaskCompletionListener(context => {
        try {
          iterator.close
        } finally {
          dataset.close
        }
      })
      iterator
    } catch {
      case t: Throwable =>
        dataset.close()
        throw t
    }
  }
}
