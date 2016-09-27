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

import java.io.DataInputStream
import java.lang.reflect.{Method, Modifier}
import java.net.URI
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}

import co.cask.cdap.api.common.RuntimeArguments
import co.cask.cdap.api.spark.{JavaSparkMain, SparkMain}
import co.cask.cdap.app.runtime.spark.distributed.{SparkCommand, SparkExecutionClient}
import co.cask.cdap.common.BadRequestException
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken
import com.google.common.base.Supplier
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.SparkConf
import org.apache.twill.common.{Cancellable, Threads}
import org.apache.twill.filesystem.{FileContextLocationFactory, Location}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
  * The main class that get submitted to Spark for execution of Spark program in CDAP.
  * The first command line argument to this class is the name of the user's Spark program class.
  */
object SparkMainWrapper {

  val ARG_USER_CLASS = "userClass"
  val ARG_EXECUTION_SERVICE_URI = "executionServiceURI"

  private val HEARTBEAT_INTERVAL_SECONDS = 1L
  private val LOG = LoggerFactory.getLogger(SparkMainWrapper.getClass)

  @volatile
  private var stopped = false
  @volatile
  private var mainThread: Option[Thread] = None

  /**
    * Triggers the shutdown of the main execution thread.
    */
  def triggerShutdown(): Unit = {
    stopped = true
    SparkRuntimeEnv.stop(mainThread)
  }

  def main(args: Array[String]): Unit = {
    mainThread = Some(Thread.currentThread)
    if (stopped) {
      return
    }

    val arguments: Map[String, String] = RuntimeArguments.fromPosixArray(args).toMap
    require(arguments.contains(ARG_USER_CLASS), "Missing Spark program class name")

    // Find the SparkRuntimeContext from the classloader. Create a new one if not found (for Spark 1.2)
    val sparkClassLoader = Try(SparkClassLoader.findFromContext()) match {
      case Success(classLoader) => classLoader
      case Failure(exception) =>
        val classLoader = SparkClassLoader.create()
        // For Spark 1.2 driver. No need to reset the classloader at the end since this is the driver process.
        SparkRuntimeUtils.setContextClassLoader(classLoader)
        classLoader
    }

    val cancellable = SparkRuntimeUtils.setContextClassLoader(sparkClassLoader)
    try {
      val runtimeContext = sparkClassLoader.getRuntimeContext

      // Load the user Spark class
      val userSparkClass = sparkClassLoader.getProgramClassLoader.loadClass(arguments(ARG_USER_CLASS))
      val executionContext = sparkClassLoader.getSparkExecutionContext(true)
      val serializableExecutionContext = new SerializableSparkExecutionContext(executionContext)
      try {
        val cancelHeartbeat = startHeartbeat(arguments, runtimeContext, () => triggerShutdown)
        try {
          userSparkClass match {
            // SparkMain
            case cls if classOf[SparkMain].isAssignableFrom(cls) =>
              cls.asSubclass(classOf[SparkMain]).newInstance().run(serializableExecutionContext)

            // JavaSparkMain
            case cls if classOf[JavaSparkMain].isAssignableFrom(cls) =>
              cls.asSubclass(classOf[JavaSparkMain]).newInstance().run(
                sparkClassLoader.createJavaExecutionContext(serializableExecutionContext))

            // main() method
            case cls =>
              getMainMethod(cls).fold(
                throw new IllegalArgumentException(userSparkClass.getName
                  + " is not a supported Spark program. It should implement either "
                  + classOf[SparkMain].getName + " or " + classOf[JavaSparkMain].getName
                  + " or has a main method defined")
              )(
                _.invoke(null, RuntimeArguments.toPosixArray(runtimeContext.getRuntimeArguments))
              )
          }
          stopped = true
        } finally {
          // If stop is request or the program returns normally, cancel the heartbeat
          if (stopped) cancelHeartbeat.cancel
        }
      } catch {
        // If there is InterruptedException after stop is being request, we don't treat it as failure
        case interrupted: InterruptedException => if (!stopped) throw interrupted
        case t: Throwable => throw t
      } finally {
        executionContext match {
          case c: AutoCloseable => c.close
          case _ => // no-op
        }
      }
    } finally {
      cancellable.cancel
    }
  }

  private def getMainMethod(obj: Class[_]): Option[Method] = {
    try {
      val mainMethod = obj.getDeclaredMethod("main", classOf[Array[String]])
      if (Modifier.isStatic(mainMethod.getModifiers)) Some(mainMethod) else None
    } catch {
      case _: Throwable => None
    }
  }

  /**
    * Starts the heartbeating thread if there is a [[co.cask.cdap.app.runtime.spark.distributed.SparkExecutionService]]
    * running.
    *
    * @param arguments the arguments to the main method
    * @param runtimeContext the [[co.cask.cdap.app.runtime.spark.SparkRuntimeContext]] for this execution
    * @return a [[org.apache.twill.common.Cancellable]] to stop the heartbeating thread and signal the completion
    *         of the execution.
    */
  private def startHeartbeat(arguments: Map[String, String],
                             runtimeContext: SparkRuntimeContext, stopFunc: () => Unit): Cancellable = {
    arguments.get(ARG_EXECUTION_SERVICE_URI).fold(new Cancellable {
      override def cancel() = {
        // no-op
      }
    })(baseURI => {
      val programRunId = runtimeContext.getProgram.getId.run(runtimeContext.getRunId.getId)
      val client = new SparkExecutionClient(URI.create(baseURI), programRunId)
      val workflowToken = Option(runtimeContext.getWorkflowInfo).map(_.getWorkflowToken).orNull
      val executor = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("heartbeat-" + programRunId.getRun))
      val credentialsUpdater = createCredentialsUpdater(runtimeContext.getConfiguration, client)

      // Make the first heartbeat. If it fails, we will not start the spark execution.
      heartbeat(client, stopFunc)

      if (!SparkRuntimeEnv.isStopped) {
        // Schedule the credentials update if necessary
        credentialsUpdater.foreach(_.startAndWait())

        // Schedule the next heartbeat
        executor.schedule(new Runnable() {
          val failureCount = new AtomicInteger

          override def run() = {
            try {
              heartbeat(client, stopFunc, workflowToken)
              failureCount.set(0)
              if (!SparkRuntimeEnv.isStopped) {
                executor.schedule(this, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS)
              }
            } catch {
              case badRequest: BadRequestException =>
                LOG.error("Invalid spark program heartbeat. Terminating the execution.", badRequest)
                stopFunc()
              case t: Throwable =>
                if (failureCount.getAndIncrement() < 10) {
                  LOG.warn("Failed to make heartbeat for {} times", failureCount.get, t)
                } else {
                  LOG.error("Failed to make heartbeat for {} times. Terminating the execution", failureCount.get)
                  stopFunc()
                }
            }
          }
        }, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS)
      }

      new Cancellable {
        override def cancel() = {
          credentialsUpdater.foreach(updater => {
            if (updater.isRunning) {
              updater.stopAndWait()
            }
          })

          executor.shutdownNow
          // Wait for the last heartbeat to complete
          executor.awaitTermination(5L, TimeUnit.SECONDS)
          // Send the complete call
          client.completed(workflowToken)
          LOG.info("Spark program execution completed: {}", programRunId)
        }
      }
    })
  }

  /**
    * Calls the heartbeat endpoint and handle the [[co.cask.cdap.app.runtime.spark.distributed.SparkCommand]]
    * returned from the call.
    */
  private def heartbeat(client: SparkExecutionClient, stopFunc: () => Unit,
                        workflowToken: BasicWorkflowToken = null) = {
    Option(client.heartbeat(workflowToken)).foreach {
      case stop if SparkCommand.STOP == stop =>
        LOG.info("Stopping Spark program upon receiving stop command")
        stopFunc()
      case notSupported => LOG.warn("Ignoring unsupported command {}", notSupported)
    }
  }

  /**
    * Creates a [[co.cask.cdap.app.runtime.spark.SparkCredentialsUpdater]] for user Credentials updates.
    */
  private def createCredentialsUpdater(hConf: Configuration,
                                       client: SparkExecutionClient): Option[SparkCredentialsUpdater] = {
    try {
      // This env variable is set by Spark for all known Spark versions
      // If it is missing, exception will be thrown
      val stagingDir = sys.env("SPARK_YARN_STAGING_DIR")
      val lf = new FileContextLocationFactory(hConf)
      val credentialsDir =
        if (stagingDir.startsWith("/")) {
          lf.create(stagingDir)
        } else {
          lf.getHomeLocation.append(stagingDir)
        }

      val sparkConf = new SparkConf
      val updateIntervalMs = sparkConf.getLong("spark.yarn.token.renewal.interval", -1L)
      if (updateIntervalMs <= 0) {
        return None
      }

      val daysToKeepFiles = sparkConf.getInt("spark.yarn.credentials.file.retention.days", 5)
      val numFilesToKeep = sparkConf.getInt("spark.yarn.credentials.file.retention.count", 5)
      val credentialsFile = credentialsDir.append("credentials-" + UUID.randomUUID().toString)

      // Update this property so that the executor will pick it up. It can't get set from the client side,
      // otherwise the AM process will try to look for keytab file
      SparkRuntimeEnv.setProperty("spark.yarn.credentials.file", credentialsFile.toURI.toString)
      Some(new SparkCredentialsUpdater(createCredentialsSupplier(client, credentialsDir), credentialsDir,
                                       credentialsFile.getName, updateIntervalMs,
                                       TimeUnit.DAYS.toMillis(daysToKeepFiles), numFilesToKeep))
    } catch {
      case t: Throwable => {
        LOG.warn("Failed to create credentials updater. Credentials update disabled", t)
        None
      }
    }
  }

  /**
    * Creates a [[com.google.common.base.Supplier]] to supply [[org.apache.hadoop.security.Credentials]] for update.
    */
  private def createCredentialsSupplier(client: SparkExecutionClient,
                                        credentialsDir: Location): Supplier[Credentials] = {
    new Supplier[Credentials] {
      override def get() = {
        // Request for the credentials to be written to a temp location
        val tmpLocation = credentialsDir.append("fetch-credentials-" + UUID.randomUUID().toString + ".tmp")
        try {
          client.writeCredentials(tmpLocation)

          // Decode the credentials, update the credentials of the current user and return it
          val credentials = new Credentials()
          val input = new DataInputStream(tmpLocation.getInputStream)
          try {
            credentials.readTokenStorageStream(input)
            UserGroupInformation.getCurrentUser.addCredentials(credentials)
            LOG.debug("Credentials updated: {}", credentials.getAllTokens)
            credentials
          } finally {
            input.close()
          }
        } finally {
          try {
            if (!tmpLocation.delete()) {
              LOG.warn("Failed to delete temporary location {}", tmpLocation)
            }
          } catch {
            case t: Throwable => LOG.warn("Exception raised when deleting temporary location {}",
                                          tmpLocation.asInstanceOf[Any], t.asInstanceOf[Any])
          }
        }
      }
    }
  }
}
