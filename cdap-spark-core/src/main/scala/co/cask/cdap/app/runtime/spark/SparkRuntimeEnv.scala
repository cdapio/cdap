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

import java.util
import java.util.Properties
import java.util.concurrent.{ConcurrentLinkedQueue, ExecutorService, TimeUnit}
import javax.annotation.Nullable

import com.google.common.reflect.TypeToken
import org.apache.spark.scheduler._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * A singleton for holding information used by the runtime system.
  * It keeps a set of global configs for [[org.apache.spark.SparkConf]]. It also maintain references to
  * the [[org.apache.spark.SparkContext]] and [[org.apache.spark.streaming.StreamingContext]]
  * to provide universal access to those contexts created by the user Spark program within CDAP.
  *
  * With ClassLoader isolation, there will be one instance of this class per one Spark program execution.
  */
object SparkRuntimeEnv {

  private val LOG = LoggerFactory.getLogger(SparkRuntimeEnv.getClass)
  private var stopped = false
  private val properties = new Properties
  private var sparkContext: Option[SparkContext] = None
  private var streamingContext: Option[StreamingContext] = None
  private val batchedWALs = new mutable.ListBuffer[AnyRef]
  private val rateControllers = new mutable.ListBuffer[AnyRef]
  private val sparkListeners = new ConcurrentLinkedQueue[SparkListener]()

  /**
    * Returns `true` if this Spark execution environment is already stopped.
    */
  def isStopped: Boolean = {
    this.synchronized {
      stopped
    }
  }

  /**
    * Sets a global property for the Spark program. The signature of this method must be the same as
    * `System.setProperty`.
    *
    * @param key property key
    * @param value property value
    */
  def setProperty(key: String, value: String): String = properties.setProperty(key, value).asInstanceOf[String]

  /**
    * Puts all global properties into the given [[org.apache.spark.SparkConf]].
    */
  def setupSparkConf(sparkConf: SparkConf): Unit = sparkConf.setAll(properties)

  /**
    * Adds a [[org.apache.spark.scheduler.SparkListener]]. The given listener will be added to
    * [[org.apache.spark.SparkContext]] when it becomes available.
    */
  def addSparkListener(listener: SparkListener): Unit = sparkListeners.add(listener)

  /**
    * Sets the [[org.apache.spark.SparkContext]] for the execution.
    */
  def setContext(context: SparkContext): Unit = {
    this.synchronized {
      if (stopped) {
        context.stop()
        throw new IllegalStateException("Spark program is already stopped")
      }

      if (sparkContext.isDefined) {
        throw new IllegalStateException("SparkContext was already created")
      }

      sparkContext = Some(context)
      context.addSparkListener(new DelegatingSparkListener)
    }
  }

  /**
    * Sets the [[org.apache.spark.streaming.StreamingContext]] that is currently in use.
    */
  def setContext(context: StreamingContext): Unit = {
    this.synchronized {
      if (stopped) {
        context.stop(false)
        throw new IllegalStateException("Spark program is already stopped")
      }

      // Spark doesn't allow multiple StreamingContext instances concurrently, hence we don't need to check in here
      streamingContext = Some(context)
    }
  }

  /**
    * Adds the reference to BatchedWriteAheadLog instance.
    */
  def addBatchedWriteAheadLog(batchedWAL: AnyRef): Unit = {
    this.synchronized {
      if (stopped) {
        stopBatchedWAL(batchedWAL)
        throw new IllegalStateException("Spark program is already stopped")
      }
      batchedWALs += batchedWAL
    }
  }

  /**
    * Adds the reference to RateController instance for freeing up ExecutionContext on completion.
    */
  def addRateController(controller: AnyRef): Unit = {
    this.synchronized {
      if (stopped) {
        stopRateController(controller)
        throw new IllegalStateException("Spark program is already stopped")
      }
      rateControllers += controller
    }
  }

  /**
    * Returns the current [[org.apache.spark.SparkContext]].
    *
    * @throws IllegalStateException if there is no SparkContext available.
    */
  def getContext: SparkContext = {
    this.synchronized {
      sparkContext.getOrElse(throw new IllegalStateException("SparkContext is not available"))
    }
  }

  /**
    * Sets a local property on the [[org.apache.spark.SparkContext]] object if available.
    *
    * @param key key of the property
    * @param value value of the property
    * @return `true` if successfully set the property; `false` otherwise
    */
  def setLocalProperty(key: String, value: String): Boolean = {
    this.synchronized {
      sparkContext.fold(false)(context => {
        context.setLocalProperty(key, value)
        true
      })
    }
  }

  /**
    * Gets a local property from the [[org.apache.spark.SparkContext]] object if available.
    *
    * @param key key of the property
    * @return the value of property of `null` if either there is no SparkContext or the key doesn't exist
    */
  @Nullable
  def getLocalProperty(key: String): String = {
    this.synchronized {
      sparkContext.map(_.getLocalProperty(key)).orNull
    }
  }

  /**
    * Stop this cache. It will stop the [[org.apache.spark.SparkContext]] in the cache and also prevent any
    * future setting of new [[org.apache.spark.SparkContext]].
    *
    * @param thread an optional Thread to interrupt upon stopping.
    * @return [[scala.Some]] [[org.apache.spark.SparkContext]] if there is a one
    */
  def stop(thread: Option[Thread] = None): Option[SparkContext] = {
    var sc: Option[SparkContext] = None
    var ssc: Option[StreamingContext] = None

    this.synchronized {
      if (!stopped) {
        stopped = true
        sc = sparkContext
        ssc = streamingContext
      }
    }

    try {
      ssc.foreach(context => {
        // If running Spark streaming, interrupt the thread first to give
        // the Spark program time to terminate gracefully.
        thread.foreach(t => {
          t.interrupt()
          t.join()
        })
        context.stop(false, false)
      })
    } finally {
      sc.foreach(context => {
        val cleanup = createCleanup(context, batchedWALs, rateControllers);
        try {
          context.stop
        } finally {
          // Just interrupt the thread to unblock any blocking call
          thread.foreach(_.interrupt())
          cleanup()
        }
      })
    }

    this.synchronized {
      sparkListeners.clear()
      sparkContext;
    }
  }

  /**
    * Creates a function that will stop and cleanup up all http servers and thread pools
    * associated with the given SparkContext.
    */
  private def createCleanup(sc: SparkContext,
                            batchedWALs: Iterable[AnyRef], rateControllers: Iterable[AnyRef]): () => Unit = {
    val closers = ArrayBuffer.empty[Option[() => Unit]]

    // Create a closer function for the file server in Spark
    // The SparkEnv is either accessed through the "env() method (1.4+) or the "env" field in the SparkContext
    // Every field access or method call are done through Option, hence if there is any changes in Spark,
    // it won't fail the execution, although it might create permgen leakage, but it also depends on Spark
    // has fixed this Thread resource leakage or not.
    closers += sc.callMethod("env").orElse(sc.getField("env")).flatMap(env =>
      env.getField("rpcEnv").flatMap(rpcEnv =>
        // The rpcEnv.fileServer() call returns either a HttpBasedFileServer, NettyStreamManager or AkkaFileServer
        // For HttpBasedFileServer or AkkaFileServer, we are interested in the "httpFileServer" field inside
        rpcEnv.callMethod("fileServer").flatMap(_.getField("httpFileServer")).flatMap(fs =>
          fs.getField("httpServer").flatMap(httpServer => createServerCloser(httpServer.getField("server"), sc))
        )
      )
    )

    // Create a closer funciton for the WebUI in Spark
    // The WebUI is either accessed through the "ui() method (1.4+) or the "ui" field in the SparkContext
    // The ui field is an Option
    closers += sc.callMethod("ui").orElse(sc.getField("ui")).flatMap(_ match {
      case o: Option[Any] => o.flatMap(ui =>
        // Get the serverInfo inside WebUI, which is an Option
        ui.getField("serverInfo").flatMap(_ match {
          case o: Option[Any] => o.flatMap(info => createServerCloser(info.getField("server"), sc))
          case _ => None
        })
      )
      case _ => None
    })

    // Create a closer function for stopping the thead in BatchedWriteAheadLog
    closers += Some(() => batchedWALs.foreach(stopBatchedWAL))

    // Create a closer function for shutting down all executor services
    closers += Some(() => rateControllers.foreach(stopRateController))

    // Creates a function that calls all closers
    () => closers.foreach(_.foreach(_()))
  }

  /**
    * Creates an optional function that will close the given server when getting called.
    */
  private def createServerCloser(server: Option[Any], sc: SparkContext): Option[() => Unit] = {
    server.flatMap(s => {
      s.callMethod("getThreadPool").map(threadPool =>
        () => {
          LOG.debug("Shutting down Server and ThreadPool used by Spark {}", sc)
          s.callMethod("stop")
          threadPool.callMethod("stop")
        }
      )
    })
  }

  /**
    * Stops the thread in the given BatchedWriteAheadLog instance
    */
  private def stopBatchedWAL(batchedWAL: AnyRef) : Unit = {
    // Get the internal record queue
    batchedWAL.getField("walWriteQueue").flatMap(queue => queue match {
      case c: util.Collection[Any] => Some(c)
      case _ => None
    }).foreach(queue => {
      // Wait until the queue is empty and the WAL is still active
      while (!queue.isEmpty &&
        batchedWAL.getField("active").filter(_.isInstanceOf[Boolean]).map(_.asInstanceOf[Boolean]).exists(b => b)) {
        TimeUnit.MILLISECONDS.sleep(100)
      }

      // Now call BatchedWriteAheadLog.close()
      batchedWAL.callMethod("close")
    })
  }

  /**
    * Stops the ExecutorContext inside the given RateController.
    */
  private def stopRateController(controller: AnyRef) : Unit = {
    // Get the internal executionContext field and call shutdownNow if it is instance of ExecutorService
    controller.getField("executionContext").flatMap(context => context match {
      case service: ExecutorService => Some(service)
      case _ => None
    }).foreach(_.shutdownNow())
  }

  /**
    * An implicit helper class for getting field and calling method on an object through reflection
    */
  private implicit class ObjectReflectionFunctions(obj: Any) {

    // Wrap it with an Option to avoid null
    private val objOption = Option(obj)

    def getField(fieldName: String): Option[_] = {
      try {
        // Find the first hit of given field in the given obj class hierarchy
        objOption.flatMap(obj => TypeToken.of(obj.getClass).getTypes.classes.map(t => {
          // If able to find the field, get the field value
          t.getRawType.getDeclaredFields.find(f => fieldName == f.getName).map(f => {
            f.setAccessible(true)
            f.get(obj)
          })
        }).find(_.isDefined).flatten)
      } catch {
        case t: Throwable => {
          LOG.warn("Unable to access field {} from object {}", fieldName, obj, t)
          None
        }
      }
    }

    def callMethod(methodName: String): Option[_] = {
      try {
        // Find the first hit of given method in the given obj class hierarchy
        objOption.flatMap(obj => TypeToken.of(obj.getClass).getTypes.classes.map(t => {
          // If able to find the method, invoke the method
          t.getRawType.getDeclaredMethods.find(m => methodName == m.getName).map(m => {
            m.setAccessible(true)
            m.invoke(obj)
          })
        }).find(_.isDefined).flatten)
      } catch {
        case t: Throwable => {
          LOG.warn("Unable to invoke method {} from object {}", methodName, obj, t)
          None
        }
      }
    }
  }

  /**
    * A delegating [[org.apache.spark.scheduler.SparkListener]] that simply dispatch all callbacks to a list
    * of [[org.apache.spark.scheduler.SparkListener]].
    */
  private class DelegatingSparkListener extends SparkListener {

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) =
      sparkListeners.foreach(_.onStageCompleted(stageCompleted))

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) =
      sparkListeners.foreach(_.onStageSubmitted(stageSubmitted))

    override def onTaskStart(taskStart: SparkListenerTaskStart) =
      sparkListeners.foreach(_.onTaskStart(taskStart))

    override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) =
      sparkListeners.foreach(_.onTaskGettingResult(taskGettingResult))

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) =
      sparkListeners.foreach(_.onTaskEnd(taskEnd))

    override def onJobStart(jobStart: SparkListenerJobStart) =
      sparkListeners.foreach(_.onJobStart(jobStart))

    override def onJobEnd(jobEnd: SparkListenerJobEnd) =
      sparkListeners.foreach(_.onJobEnd(jobEnd))

    override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) =
      sparkListeners.foreach(_.onEnvironmentUpdate(environmentUpdate))

    override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded) =
      sparkListeners.foreach(_.onBlockManagerAdded(blockManagerAdded))

    override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) =
      sparkListeners.foreach(_.onBlockManagerRemoved(blockManagerRemoved))

    override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) =
      sparkListeners.foreach(_.onUnpersistRDD(unpersistRDD))

    override def onApplicationStart(applicationStart: SparkListenerApplicationStart) =
      sparkListeners.foreach(_.onApplicationStart(applicationStart))

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) =
      sparkListeners.foreach(_.onApplicationEnd(applicationEnd))

    override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) =
      sparkListeners.foreach(_.onExecutorMetricsUpdate(executorMetricsUpdate))

    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded) =
      sparkListeners.foreach(_.onExecutorAdded(executorAdded))

    override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved) =
      sparkListeners.foreach(_.onExecutorRemoved(executorRemoved))

    // Adding noop implementation because of CDAP-5768
    def onOtherEvent(event: SparkListenerEvent) { }
  }
}
