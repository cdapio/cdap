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

package co.cask.cdap.spark.app

import java.util.concurrent.TimeUnit

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.{AbstractSpark, SparkExecutionContext, SparkMain}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
  * A Spark application for performing data operations using transactions.
  */
class TransactionSpark extends AbstractSpark with SparkMain  {

  override protected def configure() = setMainClassName(classOf[TransactionSpark].getName)

  override def run(implicit sec: SparkExecutionContext): Unit = {
    val sc = new SparkContext

    val runtimeArgs: Map[String, String] = sec.getRuntimeArguments.toMap
    val streamRDD: RDD[String] = sc.fromStream(runtimeArgs("source.stream"))
    val wordsRDD = streamRDD.flatMap(_.split("\\s+"))

    // Read the stream, split it and save it to dataset. This test the implicit transaction.
    // The key is just some generated unique ID
    wordsRDD
      .zipWithUniqueId()
      .map(t => (Bytes.toBytes(t._2), Bytes.toBytes(t._1)))
      .saveAsDataset(runtimeArgs("keyvalue.table"))

    // Read from the key value table, and collect the results. This test the implicit commit on job end transaction
    // We are only interested in the value of the key value table
    val strings = sc.fromDataset[Array[Byte], Array[Byte]](runtimeArgs("keyvalue.table"))
      .map(t => (Bytes.toString(t._2), 1))
      .reduceByKey(_ + _)
      .collect

    // Make sure it reads the same set of strings back
    val joinedCount = wordsRDD
      .map((_, 1))
      .reduceByKey(_ + _)
      .join(sc.parallelize(strings))
      .count

    require(joinedCount == strings.length)

    // Read from the key value table, perform the reduce by key and save it to result.
    // This is to test the upgrade of implicit transaction works
    sc.fromDataset[Array[Byte], Array[Byte]](runtimeArgs("keyvalue.table"))
      .map(t => (Bytes.toString(t._2), 1))
      .reduceByKey(_ + _)
      .map(t => (Bytes.toBytes(t._1), Bytes.toBytes(t._2)))
      .saveAsDataset(runtimeArgs("result.all.dataset"))

    // Use an explicit transaction to read from the all dataset and perform filtering and write it to dataset
    Transaction(() => {
      sc.fromDataset[Array[Byte], Array[Byte]](runtimeArgs("result.all.dataset"))
        .map(t => (t._1, Bytes.toInt(t._2)))
        .filter(t => t._2 >= runtimeArgs("result.threshold").toInt)
        .map(t => (t._1, Bytes.toBytes(t._2)))
        .saveAsDataset(runtimeArgs("result.threshold.dataset"))
    })

    // Sleep for 5 mins. This allows the unit-test to verify the dataset results
    // committed by the transaction above.
    // When unit-test try to stop the Spark program, this thread should get interrupted and hence terminating
    // the Spark program
    TimeUnit.SECONDS.sleep(300)
  }
}
