/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.api.spark

import io.cdap.cdap.api.annotation.Beta
import io.cdap.cdap.api.data.DatasetContext
import io.cdap.cdap.api.data.batch.Split
import io.cdap.cdap.api.{Transactional, TxRunnable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * The trait for Spark program to extend from. It provides access to [[io.cdap.cdap.api.spark.SparkExecutionContext]]
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
  *     // Create a RDD from dataset "input"
  *     val inputRDD: RDD[String] = sc.fromDataset("input").values
  *
  *     // Create a RDD from dataset "lookup", which represents a lookup table from String to Long
  *     val lookupRDD: RDD[(String, Long)] = sc.fromDataset("lookup")
  *
  *     // Join the "input" dataset with the "lookup" dataset and save it to "output" dataset
  *     inputRDD.map(body => (body, body))
  *       .inputRDD(lookupRDD)
  *       .mapValues(_._2)
  *       .saveAsDataset("output")
  *
  *     // Perform multiple operations in the same transaction
  *     Transaction(() => {
  *       // Create a standard wordcount RDD
  *       val wordCountRDD = inputRDD
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
    * data to [[io.cdap.cdap.api.dataset.Dataset]].
    *
    * @param rdd the [[org.apache.spark.rdd.RDD]] to operate on
    * @tparam K key type
    * @tparam V value type
    */
  implicit class SparkProgramRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {

    /**
      * Saves the given [[org.apache.spark.rdd.RDD]] to the given [[io.cdap.cdap.api.dataset.Dataset]].
      *
      * @param datasetName name of the Dataset
      * @param sec the [[io.cdap.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @throws io.cdap.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def saveAsDataset(datasetName: String)
                     (implicit sec: SparkExecutionContext): Unit = {
      saveAsDataset(datasetName, Map[String, String]())
    }

    /**
      * Saves the given [[org.apache.spark.rdd.RDD]] to the given [[io.cdap.cdap.api.dataset.Dataset]].
      *
      * @param namespace the namespace for the dataset
      * @param datasetName name of the Dataset
      * @param sec the [[io.cdap.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @throws io.cdap.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def saveAsDataset(namespace: String, datasetName: String)
                     (implicit sec: SparkExecutionContext): Unit = {
      saveAsDataset(namespace, datasetName, Map[String, String]())
    }

    /**
      * Saves the given [[org.apache.spark.rdd.RDD]] to the given [[io.cdap.cdap.api.dataset.Dataset]].
      *
      * @param datasetName name of the Dataset
      * @param arguments arguments for the Dataset; default is an empty [[scala.collection.Map]]
      * @param sec the [[io.cdap.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @throws io.cdap.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def saveAsDataset(datasetName: String, arguments: Map[String, String])
                     (implicit sec: SparkExecutionContext): Unit = {
      sec.saveAsDataset(rdd, datasetName, arguments)
    }

    /**
      * Saves the given [[org.apache.spark.rdd.RDD]] to the given [[io.cdap.cdap.api.dataset.Dataset]].
      *
      * @param namespace the namespace for the dataset
      * @param datasetName name of the Dataset
      * @param arguments arguments for the Dataset; default is an empty [[scala.collection.Map]]
      * @param sec the [[io.cdap.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @throws io.cdap.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
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
  implicit class SparkProgramContextFunctions(sc: SparkContext) {

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] from the given [[io.cdap.cdap.api.dataset.Dataset]].
      *
      * @param datasetName name of the Dataset
      * @tparam K key type
      * @tparam V value type
      * @return A new [[org.apache.spark.rdd.RDD]] instance that reads from the given Dataset.
      * @throws io.cdap.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def fromDataset[K: ClassTag, V: ClassTag](datasetName: String)
                                             (implicit sec: SparkExecutionContext): RDD[(K, V)] = {
      fromDataset(datasetName, Map[String, String]())
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] from the given [[io.cdap.cdap.api.dataset.Dataset]].
      *
      * @param namespace namespace in which the dataset exists
      * @param datasetName name of the Dataset
      * @tparam K key type
      * @tparam V value type
      * @return A new [[org.apache.spark.rdd.RDD]] instance that reads from the given Dataset.
      * @throws io.cdap.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def fromDataset[K: ClassTag, V: ClassTag](namespace: String,
                                              datasetName: String)
                                             (implicit sec: SparkExecutionContext): RDD[(K, V)] = {
      fromDataset(namespace, datasetName, Map[String, String]())
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] from the given [[io.cdap.cdap.api.dataset.Dataset]].
      *
      * @param datasetName name of the Dataset
      * @param arguments arguments for the Dataset; default is an empty [[scala.collection.Map]]
      * @tparam K key type
      * @tparam V value type
      * @return A new [[org.apache.spark.rdd.RDD]] instance that reads from the given Dataset.
      * @throws io.cdap.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def fromDataset[K: ClassTag, V: ClassTag](datasetName: String,
                                              arguments: Map[String, String])
                                             (implicit sec: SparkExecutionContext): RDD[(K, V)] = {
      fromDataset(datasetName, arguments, None)
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] from the given [[io.cdap.cdap.api.dataset.Dataset]].
      *
      * @param namespace namespace in which the dataset exists
      * @param datasetName name of the Dataset
      * @param arguments arguments for the Dataset; default is an empty [[scala.collection.Map]]
      * @tparam K key type
      * @tparam V value type
      * @return A new [[org.apache.spark.rdd.RDD]] instance that reads from the given Dataset.
      * @throws io.cdap.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def fromDataset[K: ClassTag, V: ClassTag](namespace: String,
                                              datasetName: String,
                                              arguments: Map[String, String])
                                             (implicit sec: SparkExecutionContext): RDD[(K, V)] = {
      fromDataset(namespace, datasetName, arguments, None)
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] from the given [[io.cdap.cdap.api.dataset.Dataset]].
      *
      * @param datasetName name of the Dataset
      * @param arguments arguments for the Dataset; default is an empty [[scala.collection.Map]]
      * @param splits an [[scala.Option]] of an [[scala.collection.Iterable]] of [[io.cdap.cdap.api.data.batch.Split]].
      *               It provided, it will be used to compute the partitions of the RDD;
      *               default is [[scala.None]] and it is up to the Dataset implementation of
      *               the [[io.cdap.cdap.api.data.batch.BatchReadable]] to decide the partitions
      * @param sec the [[io.cdap.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @tparam K key type
      * @tparam V value type
      * @return A new [[org.apache.spark.rdd.RDD]] instance that reads from the given Dataset.
      * @throws io.cdap.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def fromDataset[K: ClassTag, V: ClassTag](datasetName: String,
                                              arguments: Map[String, String],
                                              splits: Option[Iterable[_ <: Split]])
                                             (implicit sec: SparkExecutionContext): RDD[(K, V)] = {
      sec.fromDataset(sc, datasetName, arguments, splits)
    }

    /**
      * Creates a [[org.apache.spark.rdd.RDD]] from the given [[io.cdap.cdap.api.dataset.Dataset]].
      *
      * @param namespace namespace in which the dataset exists
      * @param datasetName name of the Dataset
      * @param arguments arguments for the Dataset; default is an empty [[scala.collection.Map]]
      * @param splits an [[scala.Option]] of an [[scala.collection.Iterable]] of [[io.cdap.cdap.api.data.batch.Split]].
      *               It provided, it will be used to compute the partitions of the RDD;
      *               default is [[scala.None]] and it is up to the Dataset implementation of
      *               the [[io.cdap.cdap.api.data.batch.BatchReadable]] to decide the partitions
      * @param sec the [[io.cdap.cdap.api.spark.SparkExecutionContext]] of the current execution
      * @tparam K key type
      * @tparam V value type
      * @return A new [[org.apache.spark.rdd.RDD]] instance that reads from the given Dataset.
      * @throws io.cdap.cdap.api.data.DatasetInstantiationException if the Dataset doesn't exist
      */
    def fromDataset[K: ClassTag, V: ClassTag](namespace: String,
                                              datasetName: String,
                                              arguments: Map[String, String],
                                              splits: Option[Iterable[_ <: Split]])
                                             (implicit sec: SparkExecutionContext): RDD[(K, V)] = {
      sec.fromDataset(sc, namespace, datasetName, arguments, splits)
    }
  }

  /**
    * Provides functional syntax to execute a function with a Transaction.
    */
  object Transaction extends Serializable {

    /**
      * Executes the given function in a single transaction.
      *
      * @param f the function to execute
      * @param transactional the [[io.cdap.cdap.api.Transactional]] to use for the execution
      */
    def apply[T: ClassTag](f: () => T)(implicit transactional: Transactional): T = {
      apply((context: DatasetContext) => f())
    }

    /**
      * Executes the given function in a single transaction with access to [[io.cdap.cdap.api.dataset.Dataset]]
      * through the [[io.cdap.cdap.api.data.DatasetContext]].
      *
      * @param f the function to execute
      * @param transactional the [[io.cdap.cdap.api.Transactional]] to use for the execution
      */
    def apply[T: ClassTag](f: (DatasetContext) => T)(implicit transactional: Transactional): T = {
      val result = new Array[T](1)
      transactional.execute(new TxRunnable {
        override def run(context: DatasetContext) = result(0) = f(context)
      })
      result(0)
    }
  }
}
