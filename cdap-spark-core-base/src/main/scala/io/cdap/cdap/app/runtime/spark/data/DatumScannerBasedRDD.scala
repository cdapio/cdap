/*
 * Copyright © 2017-2021 Cask Data, Inc.
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

import io.cdap.cdap.api.data.batch.Split
import io.cdap.cdap.api.dataset.Dataset
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContextProvider
import io.cdap.cdap.app.runtime.spark.SparkTransactionClient
import io.cdap.cdap.data2.metadata.lineage.AccessType
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.TaskCompletionListener
import org.apache.tephra.TransactionAware

import java.net.URI
import java.util.concurrent.TimeUnit

import scala.annotation.meta.param
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * A base implementation of [[org.apache.spark.rdd.RDD]] that iterates based
  * on [[io.cdap.cdap.app.runtime.spark.data.DatumScanner]].
  */
abstract class DatumScannerBasedRDD[R: ClassTag](@(transient @param) sc: SparkContext,
                                                 namespace: String,
                                                 datasetName: String,
                                                 arguments: Map[String, String],
                                                 @(transient @param) splits: Iterable[_ <: Split],
                                                 txServiceBaseURI: Broadcast[URI]) extends RDD[R](sc, Nil) {

  final override protected def getPartitions: Array[Partition] = {
    splits.zipWithIndex.map(t => new SplitPartition(id, t._2, t._1)).toArray
  }

  final override def compute(partition: Partition, context: TaskContext): Iterator[R] = {
    val split = partition.asInstanceOf[SplitPartition].split
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

      // Create an iterator from the split
      val iterator = new DatumScannerIterator[R](context, createDatumScanner(dataset, split))
      // Need a val as workaround for scala 2.12 https://github.com/scala/bug/issues/11016
      val listener = new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext) =  {
        try {
          iterator.close
        } finally {
          dataset.close
        }
      }}
      context.addTaskCompletionListener(listener)
      iterator
    } catch {
      case t: Throwable =>
        dataset.close()
        throw t
    }
  }

  /**
    * Creates a [[io.cdap.cdap.app.runtime.spark.data.DatumScanner]] from the given [[io.cdap.cdap.api.dataset.Dataset]]
    * and [[io.cdap.cdap.api.data.batch.Split]]. This is called from the `compute` method in the executor node.
    */
  protected def createDatumScanner(dataset: Dataset, split: Split): DatumScanner[R]
}
