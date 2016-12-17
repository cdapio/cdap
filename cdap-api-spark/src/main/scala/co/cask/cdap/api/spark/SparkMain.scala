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

import co.cask.cdap.api.annotation.Beta
import co.cask.cdap.api.data.DatasetContext
import co.cask.cdap.api.data.batch.Split
import co.cask.cdap.api.data.format.FormatSpecification
import co.cask.cdap.api.flow.flowlet.StreamEvent
import co.cask.cdap.api.stream.GenericStreamEventData
import co.cask.cdap.api.{Transactional, TxRunnable}
import org.apache.spark.SparkContext
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
  *
  *     // Perform multiple operations in the same transaction
  *     Transaction(() => {
  *       // Create a standard wordcount RDD
  *       val wordCountRDD = streamRDD
  *         .flatMap(_.split(" "))
  *         .map((_, 1))
  *         .reduceByKey(_ + _)
  *
  *       // Save those words that have count > 10 to "aboveten" dataset
  *       wordCountRDD
  *         .filter(_._2 > 10)
  *         .saveAsDataset("aboveten")
  *
  *       // Save all wordcount to another "allcounts" dataset
  *       wordCountRDD.saveAsDataset("allcounts")
  *
  *       // Updates to both "aboveten" and "allcounts" dataset will be committed within the same Transaction.
  *     })
  *
  *     // Access to Dataset instance in the driver can be done through Transaction as well
  *     Transaction((context: DatasetContext) => {
  *       val kvTable: KeyValueTable = context.getDataset("myKvTable")
  *       ...
  *     })
  *   }
  * }
  * }}}
  *
  * This interface extends serializable because the closures are anonymous class in Scala and Spark Serializes the
  * closures before sending it to worker nodes. This serialization of inner anonymous class expects the outer
  * containing class to be serializable else [[java.io.NotSerializableException]] is thrown. Having this interface
  * serializable gives a neater API.
  */
@Beta
trait SparkMain extends Serializable {

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
  protected implicit class SparkProgramRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {

    /**
      * Saves the given [[org.apache.spark.rdd.RDD]] to the given [[co.cask.cdap.api.dataset.Dataset]].
      *
      * @param datasetName name of the Dataset
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def saveAsDataset(datasetName: String)
                     (implicit sec: SparkExecutionContext): Unit = {
      saveAsDataset(datasetName, Map[String, String]())
    }

    /**
      * Saves the given [[org.apache.spark.rdd.RDD]] to the given [[co.cask.cdap.api.dataset.Dataset]].
      *
      * @param namespace the namespace for the dataset
      * @param datasetName name of the Dataset
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def saveAsDataset(namespace: String, datasetName: String)
                     (implicit sec: SparkExecutionContext): Unit = {
      saveAsDataset(namespace, datasetName, Map[String, String]())
    }

    /**
      * Saves the given [[org.apache.spark.rdd.RDD]] to the given [[co.cask.cdap.api.dataset.Dataset]].
      *
      * @param datasetName name of the Dataset
      * @param arguments arguments for the Dataset; default is an empty [[scala.collection.Map]]
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def saveAsDataset(datasetName: String, arguments: Map[String, String])
                     (implicit sec: SparkExecutionContext): Unit = {
      sec.saveAsDataset(rdd, datasetName, arguments)
    }

    /**
      * Saves the given [[org.apache.spark.rdd.RDD]] to the given [[co.cask.cdap.api.dataset.Dataset]].
      *
      * @param namespace the namespace for the dataset
      * @param datasetName name of the Dataset
      * @param arguments arguments for the Dataset; default is an empty [[scala.collection.Map]]
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def saveAsDataset(namespace: String, datasetName: String, arguments: Map[String, String])
                     (implicit sec: SparkExecutionContext): Unit = {
      sec.saveAsDataset(rdd, namespace, datasetName, arguments)
    }
  }

  /**
    * Implicit class for adding addition methods to [[org.apache.spark.SparkContext]].
    *
    * @param sc the [[org.apache.spark.SparkContext]]
    */
  protected implicit class SparkProgramContextFunctions(sc: SparkContext) {

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] from the given [[co.cask.cdap.api.dataset.Dataset]].
      *
      * @param datasetName name of the Dataset
      * @tparam K key type
      * @tparam V value type
      * @return A new [[org.apache.spark.rdd.RDD]] instance that reads from the given Dataset.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def fromDataset[K: ClassTag, V: ClassTag](datasetName: String)
                                             (implicit sec: SparkExecutionContext): RDD[(K, V)] = {
      fromDataset(datasetName, Map[String, String]())
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] from the given [[co.cask.cdap.api.dataset.Dataset]].
      *
      * @param namespace namespace in which the dataset exists
      * @param datasetName name of the Dataset
      * @tparam K key type
      * @tparam V value type
      * @return A new [[org.apache.spark.rdd.RDD]] instance that reads from the given Dataset.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def fromDataset[K: ClassTag, V: ClassTag](namespace: String,
                                              datasetName: String)
                                             (implicit sec: SparkExecutionContext): RDD[(K, V)] = {
      fromDataset(namespace, datasetName, Map[String, String]())
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] from the given [[co.cask.cdap.api.dataset.Dataset]].
      *
      * @param datasetName name of the Dataset
      * @param arguments arguments for the Dataset; default is an empty [[scala.collection.Map]]
      * @tparam K key type
      * @tparam V value type
      * @return A new [[org.apache.spark.rdd.RDD]] instance that reads from the given Dataset.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def fromDataset[K: ClassTag, V: ClassTag](datasetName: String,
                                              arguments: Map[String, String])
                                             (implicit sec: SparkExecutionContext): RDD[(K, V)] = {
      fromDataset(datasetName, arguments, None)
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] from the given [[co.cask.cdap.api.dataset.Dataset]].
      *
      * @param namespace namespace in which the dataset exists
      * @param datasetName name of the Dataset
      * @param arguments arguments for the Dataset; default is an empty [[scala.collection.Map]]
      * @tparam K key type
      * @tparam V value type
      * @return A new [[org.apache.spark.rdd.RDD]] instance that reads from the given Dataset.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def fromDataset[K: ClassTag, V: ClassTag](namespace: String,
                                              datasetName: String,
                                              arguments: Map[String, String])
                                             (implicit sec: SparkExecutionContext): RDD[(K, V)] = {
      fromDataset(namespace, datasetName, arguments, None)
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] from the given [[co.cask.cdap.api.dataset.Dataset]].
      *
      * @param datasetName name of the Dataset
      * @param arguments arguments for the Dataset; default is an empty [[scala.collection.Map]]
      * @param splits an [[scala.Option]] of an [[scala.collection.Iterable]] of [[co.cask.cdap.api.data.batch.Split]].
      *               It provided, it will be used to compute the partitions of the RDD;
      *               default is [[scala.None]] and it is up to the Dataset implementation of
      *               the [[co.cask.cdap.api.data.batch.BatchReadable]] to decide the partitions
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @tparam K key type
      * @tparam V value type
      * @return A new [[org.apache.spark.rdd.RDD]] instance that reads from the given Dataset.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def fromDataset[K: ClassTag, V: ClassTag](datasetName: String,
                                              arguments: Map[String, String],
                                              splits: Option[Iterable[_ <: Split]])
                                             (implicit sec: SparkExecutionContext): RDD[(K, V)] = {
      sec.fromDataset(sc, datasetName, arguments, splits)
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] from the given [[co.cask.cdap.api.dataset.Dataset]].
      *
      * @param namespace namespace in which the dataset exists
      * @param datasetName name of the Dataset
      * @param arguments arguments for the Dataset; default is an empty [[scala.collection.Map]]
      * @param splits an [[scala.Option]] of an [[scala.collection.Iterable]] of [[co.cask.cdap.api.data.batch.Split]].
      *               It provided, it will be used to compute the partitions of the RDD;
      *               default is [[scala.None]] and it is up to the Dataset implementation of
      *               the [[co.cask.cdap.api.data.batch.BatchReadable]] to decide the partitions
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @tparam K key type
      * @tparam V value type
      * @return A new [[org.apache.spark.rdd.RDD]] instance that reads from the given Dataset.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def fromDataset[K: ClassTag, V: ClassTag](namespace: String,
                                              datasetName: String,
                                              arguments: Map[String, String],
                                              splits: Option[Iterable[_ <: Split]])
                                             (implicit sec: SparkExecutionContext): RDD[(K, V)] = {
      sec.fromDataset(sc, namespace, datasetName, arguments, splits)
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] that represents data from the given stream for events in the given
      * time range.
      *
      * @param streamName name of the stream
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @param decoder a function to convert a [[co.cask.cdap.api.flow.flowlet.StreamEvent]] to a value of type `T`
      * @tparam T value type
      * @return a new [[org.apache.spark.rdd.RDD]] instance that reads from the given stream.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Stream doesn't exist
      */
    def fromStream[T: ClassTag](streamName: String)
                               (implicit sec: SparkExecutionContext, decoder: StreamEvent => T): RDD[T] = {
      fromStream(streamName, 0L, Long.MaxValue)
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] that represents data from the given stream for events in the given
      * time range.
      *
      * @param namespace namespace in which the stream exists
      * @param streamName name of the stream
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @param decoder a function to convert a [[co.cask.cdap.api.flow.flowlet.StreamEvent]] to a value of type `T`
      * @tparam T value type
      * @return a new [[org.apache.spark.rdd.RDD]] instance that reads from the given stream.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Stream doesn't exist
      */
    def fromStream[T: ClassTag](namespace: String, streamName: String)
                               (implicit sec: SparkExecutionContext, decoder: StreamEvent => T): RDD[T] = {
      fromStream(namespace, streamName, 0L, Long.MaxValue)
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
      * @param decoder a function to convert a [[co.cask.cdap.api.flow.flowlet.StreamEvent]] to a value of type `T`
      * @tparam T value type
      * @return a new [[org.apache.spark.rdd.RDD]] instance that reads from the given stream.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Stream doesn't exist
      */
    def fromStream[T: ClassTag](streamName: String, startTime: Long, endTime: Long)
                               (implicit sec: SparkExecutionContext, decoder: StreamEvent => T): RDD[T] = {
      sec.fromStream(sc, streamName, startTime, endTime)
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] that represents data from the given stream for events in the given
      * time range.
      *
      * @param namespace namespace in which the stream exists
      * @param streamName name of the stream
      * @param startTime the starting time of the stream to be read in milliseconds (inclusive);
      *                  default is 0, which means reading from beginning of the stream.
      * @param endTime the ending time of the streams to be read in milliseconds (exclusive);
      *                default is [[scala.Long.MaxValue]], which means reading till the last event.
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @param decoder a function to convert a [[co.cask.cdap.api.flow.flowlet.StreamEvent]] to a value of type `T`
      * @tparam T value type
      * @return a new [[org.apache.spark.rdd.RDD]] instance that reads from the given stream.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Stream doesn't exist
      */
    def fromStream[T: ClassTag](namespace: String, streamName: String, startTime: Long, endTime: Long)
                               (implicit sec: SparkExecutionContext, decoder: StreamEvent => T): RDD[T] = {
      sec.fromStream(sc, namespace, streamName, startTime, endTime)
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] that represents all data from the given stream.
      * The data in the RDD is always a pair, with the first entry as a [[scala.Long]], representing the
      * event timestamp, while the second entry is a [[co.cask.cdap.api.stream.GenericStreamEventData]],
      * which contains data decoded from the stream event body based on
      * the given [[co.cask.cdap.api.data.format.FormatSpecification]].
      *
      * @param streamName name of the stream
      * @param formatSpec the [[co.cask.cdap.api.data.format.FormatSpecification]] describing the format in the stream
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @tparam T value type
      * @return a new [[org.apache.spark.rdd.RDD]] instance that reads from the given stream.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Stream doesn't exist
      */
    def fromStream[T: ClassTag](streamName: String, formatSpec: FormatSpecification)
                               (implicit sec: SparkExecutionContext) : RDD[(Long, GenericStreamEventData[T])] = {
      fromStream(streamName, formatSpec, 0, Long.MaxValue)
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] that represents all data from the given stream.
      * The data in the RDD is always a pair, with the first entry as a [[scala.Long]], representing the
      * event timestamp, while the second entry is a [[co.cask.cdap.api.stream.GenericStreamEventData]],
      * which contains data decoded from the stream event body based on
      * the given [[co.cask.cdap.api.data.format.FormatSpecification]].
      *
      * @param namespace namespace in which the stream exists
      * @param streamName name of the stream
      * @param formatSpec the [[co.cask.cdap.api.data.format.FormatSpecification]] describing the format in the stream
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @tparam T value type
      * @return a new [[org.apache.spark.rdd.RDD]] instance that reads from the given stream.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Stream doesn't exist
      */
    def fromStream[T: ClassTag](namespace: String, streamName: String, formatSpec: FormatSpecification)
                               (implicit sec: SparkExecutionContext) : RDD[(Long, GenericStreamEventData[T])] = {
      fromStream(namespace, streamName, formatSpec, 0, Long.MaxValue)
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] that represents data from the given stream for events in the given
      * time range. The data in the RDD is always a pair, with the first entry as a [[scala.Long]], representing the
      * event timestamp, while the second entry is a [[co.cask.cdap.api.stream.GenericStreamEventData]],
      * which contains data decoded from the stream event body based on
      * the given [[co.cask.cdap.api.data.format.FormatSpecification]].
      *
      * @param streamName name of the stream
      * @param formatSpec the [[co.cask.cdap.api.data.format.FormatSpecification]] describing the format in the stream
      * @param startTime the starting time of the stream to be read in milliseconds (inclusive)
      * @param endTime the ending time of the streams to be read in milliseconds (exclusive)
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @tparam T value type
      * @return a new [[org.apache.spark.rdd.RDD]] instance that reads from the given stream.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Stream doesn't exist
      */
    def fromStream[T: ClassTag](streamName: String, formatSpec: FormatSpecification,
                                startTime: Long, endTime: Long)
                               (implicit sec: SparkExecutionContext) : RDD[(Long, GenericStreamEventData[T])] = {
      sec.fromStream(sc, streamName, formatSpec, startTime, endTime)
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] that represents data from the given stream for events in the given
      * time range. The data in the RDD is always a pair, with the first entry as a [[scala.Long]], representing the
      * event timestamp, while the second entry is a [[co.cask.cdap.api.stream.GenericStreamEventData]],
      * which contains data decoded from the stream event body based on
      * the given [[co.cask.cdap.api.data.format.FormatSpecification]].
      *
      * @param namespace namespace in which the stream exists
      * @param streamName name of the stream
      * @param formatSpec the [[co.cask.cdap.api.data.format.FormatSpecification]] describing the format in the stream
      * @param startTime the starting time of the stream to be read in milliseconds (inclusive)
      * @param endTime the ending time of the streams to be read in milliseconds (exclusive)
      * @param sec the [[co.cask.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @tparam T value type
      * @return a new [[org.apache.spark.rdd.RDD]] instance that reads from the given stream.
      * @throws co.cask.cdap.api.data.DatasetInstantiationException if the Stream doesn't exist
      */
    def fromStream[T: ClassTag](namespace: String, streamName: String, formatSpec: FormatSpecification,
                                startTime: Long, endTime: Long)
                               (implicit sec: SparkExecutionContext) : RDD[(Long, GenericStreamEventData[T])] = {
      sec.fromStream(sc, streamName, formatSpec, startTime, endTime)
    }
  }

  /**
    * Provides functional syntax to execute a function with a Transaction.
    */
  protected object Transaction extends Serializable {

    /**
      * Executes the given function in a single transaction.
      *
      * @param f the function to execute
      * @param transactional the [[co.cask.cdap.api.Transactional]] to use for the execution
      */
    def apply[T: ClassTag](f: () => T)(implicit transactional: Transactional): T = {
      apply((context: DatasetContext) => f())
    }

    /**
      * Executes the given function in a single transaction with access to [[co.cask.cdap.api.dataset.Dataset]]
      * through the [[co.cask.cdap.api.data.DatasetContext]].
      *
      * @param f the function to execute
      * @param transactional the [[co.cask.cdap.api.Transactional]] to use for the execution
      */
    def apply[T: ClassTag](f: (DatasetContext) => T)(implicit transactional: Transactional): T = {
      val result = new Array[T](1)
      transactional.execute(new TxRunnable {
        override def run(context: DatasetContext) = result(0) = f(context)
      })
      result(0)
    }
  }

  /**
    * An implicit object that transforms [[co.cask.cdap.api.flow.flowlet.StreamEvent]] to a
    * [[scala.Tuple2]] of (eventTimestamp, UTF-8 decoded body string).
    */
  protected implicit val timestampStringStreamDecoder: (StreamEvent) => (Long, String) = (e: StreamEvent) => {
    (e.getTimestamp, Charset.forName("UTF-8").decode(e.getBody).toString)
  }

  /**
    * An implicit object that transforms [[co.cask.cdap.api.flow.flowlet.StreamEvent]] body to a UTF-8 string.
    */
  protected implicit val stringStreamDecoder: (StreamEvent) => String = (e: StreamEvent) =>
    timestampStringStreamDecoder(e)._2
}
