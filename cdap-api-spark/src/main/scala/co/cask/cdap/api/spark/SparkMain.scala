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

package co.cask.cdap.api.spark

import java.nio.charset.Charset

import co.cask.cdap.api.data.batch.Split
import co.cask.cdap.api.flow.flowlet.StreamEvent
import org.apache.spark
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * The trait for Spark program to extend from. It provides access to [[co.cask.cdap.api.spark.SparkExecutionContext]]
  * for interacting with CDAP. It also provides extra functions to [[org.apache.spark.SparkContext]] and
  * [[org.apache.spark.rdd.RDD]] through implicit objects.
  *
  * Example:
  * {{{
  * class MySparkMain extends SparkMain {
  *
  *   override def run(implicit sec: SparkExecutionContext) = {
  *     val sc = new SparkContext
  *
  *     // Create a RDD from stream "input", with event body decoded as UTF-8 String
  *     val streamRDD: RDD[String] = sc.fromStream("input")
  *
  *     // Create a RDD from dataset "lookup", which represents a lookup table from String to Long
  *     val lookupRDD: RDD[(String, Long)] = sc.fromDataset("lookup")
  *
  *     // Join the "input" stream with the "lookup" dataset and save it to "output" dataset
  *     streamRDD.map(body => (body, body))
  *       .join(lookupRDD)
  *       .mapValues(_._2)
  *       .saveAsDataset("output")
  *   }
  * }
  * }}}
  */
trait SparkMain {

  /**
    * This method will be called when the Spark program starts.
    *
    * @param sec the implicit context for interacting with CDAP
    */
  def run(implicit sec: SparkExecutionContext): Unit

  /**
    * Implicit class for adding methods to [[org.apache.spark.rdd.RDD]] for saving
    * data to [[co.cask.cdap.api.dataset.Dataset]].
    *
    * @param rdd the [[org.apache.spark.rdd.RDD]] to operate on
    * @tparam K key type
    * @tparam V value type
    */
  protected implicit class SparkProgramRDDFunctions[K, V](rdd: RDD[(K, V)]) {

    /**
      * Saves the given [[org.apache.spark.rdd.RDD]] to the given [[co.cask.cdap.api.dataset.Dataset]].
      *
      * @param datasetName name of the Dataset
      * @param arguments arguments for the Dataset; default is an empty [[scala.collection.Map]]
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def saveAsDataset(datasetName: String, arguments: Map[String, String] = Map())
                     (implicit sec: SparkExecutionContext): Unit = {
      sec.saveAsDataset(rdd, datasetName, arguments)
    }
  }

  /**
    * Implicit class for adding addition methods to [[org.apache.spark.SparkContext]].
    *
    * @param sc the [[org.apache.spark.SparkContext]]
    */
  protected implicit class SparkProgramContextFunctions(sc: spark.SparkContext) {

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] from the given [[co.cask.cdap.api.dataset.Dataset]].
      *
      * @param datasetName name of the Dataset
      * @param arguments arguments for the Dataset; default is an empty [[scala.collection.Map]]
      * @param splits an [[scala.Option]] of an [[scala.collection.Iterable]] of [[co.cask.cdap.api.data.batch.Split]].
      *               It provided, it will be used to compute the partitions of the RDD;
      *               default is [[scala.None] and it is up to the Dataset implementation of
      *               the [[co.cask.cdap.api.data.batch.BatchReadable]] to decide the partitions
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @tparam K key type
      * @tparam V value type
      * @return A new [[org.apache.spark.rdd.RDD]] instance that reads from the given Dataset.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def fromDataset[K: ClassTag, V: ClassTag](datasetName: String,
                                              arguments: Map[String, String] = Map(),
                                              splits: Option[Iterable[_ <: Split]] = None)
                                             (implicit sec: SparkExecutionContext): RDD[(K, V)] = {
      sec.fromDataset(sc, datasetName, arguments, splits)
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] that represents data from the given stream for events in the given
      * time range.
      *
      * @param streamName name of the stream
      * @param startTime the starting time of the stream to be read in milliseconds (inclusive);
      *                  default is 0, which means reading from beginning of the stream.
      * @param endTime the ending time of the streams to be read in milliseconds (exclusive);
      *                default is [[scala.Long.MaxValue]], which means reading till the last event.
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @param decoder a function to convert a [[co.cask.cdap.api.flow.flowlet.StreamEvent]] to a value
      * @tparam T value type
      * @return a new [[org.apache.spark.rdd.RDD]] instance that reads from the given stream.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Stream doesn't exist
      */
    def fromStream[T: ClassTag](streamName: String, startTime: Long = 0L, endTime: Long = Long.MaxValue)
                               (implicit sec: SparkExecutionContext, decoder: StreamEvent => T): RDD[T] = {
      sec.fromStream(sc, streamName, startTime, endTime)
    }
  }

  /**
    * An implicit object that performs identity transform for [[co.cask.cdap.api.flow.flowlet.StreamEvent]].
    */
  protected implicit val identityStreamDecoder = (e: StreamEvent) => e

  /**
    * An implicit object that transforms [[co.cask.cdap.api.flow.flowlet.StreamEvent]] to a
    * [[scala.Tuple2]] of (eventTimestamp, UTF-8 decoded body string).
    */
  protected implicit val timestampStringStreamDecoder = (e: StreamEvent) => {
    (e.getTimestamp, Charset.forName("UTF-8").decode(e.getBody).toString)
  }

  /**
    * An implicit object that transforms [[co.cask.cdap.api.flow.flowlet.StreamEvent]] body to a UTF-8 string.
    */
  protected implicit val stringStreamDecoder = (e: StreamEvent) =>
    timestampStringStreamDecoder(e)._2
}
