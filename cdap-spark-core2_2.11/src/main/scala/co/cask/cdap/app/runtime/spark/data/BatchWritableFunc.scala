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

import co.cask.cdap.api.data.batch.BatchWritable
import co.cask.cdap.api.dataset.Dataset
import co.cask.cdap.app.runtime.spark.{SparkRuntimeContextProvider, SparkTransactionClient}
import co.cask.cdap.data2.metadata.lineage.AccessType
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.tephra.TransactionAware

import scala.collection.JavaConversions._

/**
  * In Spark2, the context output metrics are not available, so this version doesn't do anything with metrics
  */
object BatchWritableFunc {

  /**
    * Creates a function used by [[org.apache.spark.SparkContext]] runJob for writing data to a
    * Dataset that implements [[co.cask.cdap.api.data.batch.BatchWritable]]. The returned function
    * will be executed in executor nodes.
    */
  private[spark] def create[K, V](namespace: String,
                                  datasetName: String,
                                  arguments: Map[String, String],
                                  txServiceBaseURI: Broadcast[URI]) = (context: TaskContext,
                                                                       itor: Iterator[(K, V)]) => {

    val sparkTxClient = new SparkTransactionClient(txServiceBaseURI.value)
    val datasetCache = SparkRuntimeContextProvider.get().getDatasetCache
    val dataset: Dataset = datasetCache.getDataset(namespace, datasetName, arguments, true, AccessType.WRITE)

    try {
      // Creates an Option[TransactionAware] if the dataset is a TransactionAware
      val txAware = dataset match {
        case txAware: TransactionAware => Some(txAware)
        case _ => None
      }

      // Try to get the transaction for this stage. Hardcoded the timeout to 10 seconds for now
      txAware.foreach(_.startTx(sparkTxClient.getTransaction(context.stageId(), 10, TimeUnit.SECONDS)))

      // Write through BatchWritable.
      val writable = dataset.asInstanceOf[BatchWritable[K, V]]
      var records = 0
      while (itor.hasNext) {
        val pair = itor.next()
        writable.write(pair._1, pair._2)

        // Periodically calling commitTx to flush changes. Hardcoded to 1000 records for now
        if (records > 1000) {
          txAware.foreach(_.commitTx())
          records = 0
        }
        records += 1
      }

      // Flush all writes
      txAware.foreach(_.commitTx())
    } finally {
      dataset.close()
    }
  }
}
