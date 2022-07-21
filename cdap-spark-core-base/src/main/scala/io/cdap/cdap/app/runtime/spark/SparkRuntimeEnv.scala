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

package io.cdap.cdap.app.runtime.spark

import com.google.common.reflect.TypeToken
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory

import java.lang.Thread.UncaughtExceptionHandler
import java.util
import java.util.Properties
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import javax.annotation.Nullable

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
  private val sparkContext = new CompletableFuture[SparkContext]()
  private val streamingContext = new CompletableFuture[StreamingContext]()
  private val batchedWALs = new mutable.ListBuffer[AnyRef]
  private val rateControllers = new mutable.ListBuffer[AnyRef]
  private val pyMonitorThreads = new mutable.ListBuffer[Thread]
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
    * Returns the value of a property set via the `setProperty` method before.
    *
    * @param key property key
    * @return the property value or `null`
    */
  def getProperty(key: String): String = properties.getProperty(key)

  /**
    * Puts all global properties into the given [[org.apache.spark.SparkConf]].
    */
  def setupSparkConf(sparkConf: SparkConf): Unit = properties.foreach(t => sparkConf.set(t._1, t._2))

  /**
    * Adds a [[org.apache.spark.scheduler.SparkListener]].
    */
  def addSparkListener(listener: SparkListener): Unit = sparkListeners.add(listener)

  /**
    * Returns the current list of [[org.apache.spark.scheduler.SparkListener]] added through the
    * `addListener` method
    */
  def getSparkListeners(): Seq[SparkListener] = sparkListeners.toSeq

  /**
    * Sets the [[org.apache.spark.SparkContext]] for the execution.
    */
  def setContext(context: SparkContext): Unit = {
    this.synchronized {
      if (stopped) {
        context.stop()
        throw new IllegalStateException("Spark program is already stopped")
      }

      if (sparkContext.isDone) {
        throw new IllegalStateException("SparkContext was already created")
      }

      sparkContext.complete(context)
    }

    // For Spark 1.2, it doesn't support `spark.extraListeners` setting.
    // We need to add the listener here and simulate a call to the onApplicationStart.
    if (context.version == "1.2" || context.version.startsWith("1.2.")) {
      val listener = new DelegatingSparkListener
      context.addSparkListener(listener)
      val applicationStart = new SparkListenerApplicationStart(context.appName, Some(context.applicationId),
                                                               context.startTime, context.sparkUser,
                                                               context.applicationAttemptId, None)
      listener.onApplicationStart(applicationStart)
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
      streamingContext.complete(context)
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
    * Adds the reference to MonitorThread instance for interrupting the thread on completion.
    * @param thread
    */
  def addPyMonitorThread(thread: Thread): Unit = {
    this.synchronized {
      thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler {
        override def uncaughtException(t: Thread, e: Throwable): Unit = {
          e match {
            case e: InterruptedException => LOG.trace("Thread {} interrupted", t)
            case e => LOG.warn("Exception raised from {}", t: Any, e: Any)
          }
        }
      })
      if (stopped) {
        thread.interrupt()
        throw new IllegalStateException("Spark program is already stopped")
      }
      pyMonitorThreads += thread
    }
  }

  /**
    * Returns the current [[org.apache.spark.SparkContext]].
    *
    * @throws IllegalStateException if there is no SparkContext available.
    */
  def getContext: SparkContext = {
    Option.apply(sparkContext.getNow(null)).getOrElse(throw new IllegalStateException("SparkContext is not available"))
  }

  /**
    * Waits for the current [[org.apache.spark.SparkContext]] to be available and returns it. Note that this method
    * shouldn't be called from the same thread that the [[org.apache.spark.SparkContext]] is being constructed,
    * otherwise deadlock might occur.
    */
  def waitForContext: SparkContext = {
    sparkContext.get
  }

  /**
    * Returns an [[scala.Option]] which contains the [[org.apache.spark.streaming.StreamingContext]] if there one.
    */
  def getStreamingContext: Option[StreamingContext] = {
    Option.apply(streamingContext.getNow(null))
  }

  /**
    * Sets a local property on the [[org.apache.spark.SparkContext]] object if available.
    *
    * @param key key of the property
    * @param value value of the property
    * @return `true` if successfully set the property; `false` otherwise
    */
  def setLocalProperty(key: String, value: String): Boolean = {
    Option.apply(sparkContext.getNow(null)).fold(false)(context => {
      context.setLocalProperty(key, value)
      true
    })
  }

  /**
    * Gets a local property from the [[org.apache.spark.SparkContext]] object if available.
    *
    * @param key key of the property
    * @return the value of property of `null` if either there is no SparkContext or the key doesn't exist
    */
  @Nullable
  def getLocalProperty(key: String): String = {
    Option.apply(sparkContext.getNow(null)).map(_.getLocalProperty(key)).orNull
  }

  /**
    * Stop this cache. It will stop the [[org.apache.spark.SparkContext]] in the cache and also prevent any
    * future setting of new [[org.apache.spark.SparkContext]].
    *
    * @return [[scala.Some]] [[org.apache.spark.SparkContext]] if there is a one
    */
  def stop(): Option[SparkContext] = {
    this.synchronized {
      if (!stopped) {
        stopped = true
      }
    }

    val sc = Option.apply(sparkContext.getNow(null))
    val ssc = Option.apply(streamingContext.getNow(null))

    try {
      ssc.foreach(_.stop(false, false))
    } finally {
      sc.foreach(context => {
        val cleanup = createCleanup(context, batchedWALs, rateControllers, pyMonitorThreads);
        try {
          context.stop
        } finally {
          cleanup()
        }
      })
    }

    this.synchronized {
      sparkListeners.clear()
    }
    sc
  }

  /**
    * Creates a function that will stop and cleanup up all http servers and thread pools
    * associated with the given SparkContext.
    */
  private def createCleanup(sc: SparkContext,
                            batchedWALs: Iterable[AnyRef],
                            rateControllers: Iterable[AnyRef],
                            pyMonitorThreads: Iterable[Thread]): () => Unit = {
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

    // Create a closer function for interrupting MonitorThread
    closers += Some(() => pyMonitorThreads.foreach(t => {
      while (t.isAlive) {
        LOG.debug("Interrupting {}", t)
        t.interrupt()
        // Wait for 200ms for the thread to terminate. If not, try to interrupt it again.
        // The monitor thread has a 10 seconds sleep after it do its work.
        // So this loop should be able to kill the thread.
        t.join(200)
      }
    }))

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

      // Now call BatchedWriteAheadLog.realClose()
      // This method is created by the SparkClassRewriter
      batchedWAL.callMethod("realClose")
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
          // causes a strange compilation error in spark2 if we don't use obj.toString
          LOG.trace("Unable to access field {} from object {}", fieldName, obj.toString, t)
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
          // causes a strange compilation error in spark2 if we don't use obj.toString
          LOG.trace("Unable to invoke method {} from object {}", methodName, obj.toString, t)
          None
        }
      }
    }
  }

}
