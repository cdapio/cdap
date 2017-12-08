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

import co.cask.cdap.api.common.RuntimeArguments
import co.cask.cdap.api.spark.JavaSparkMain
import co.cask.cdap.api.spark.SparkMain
import org.apache.twill.common.Cancellable
import org.slf4j.LoggerFactory

import java.lang.reflect.Method
import java.lang.reflect.Modifier

/**
  * The main class that get submitted to Spark for execution of Spark program in CDAP.
  * The first command line argument to this class is the name of the user's Spark program class.
  */
object SparkMainWrapper {

  private val LOG = LoggerFactory.getLogger(SparkMainWrapper.getClass)

  @volatile
  private var stopped = false

  @volatile
  var cancellable = new Cancellable {
    override def cancel(): Unit = {
      stopped = true
    }
  }

  def main(args: Array[String]): Unit = {
    if (stopped) {
      return
    }

    // Initialize the Spark runtime and update the Cancellable.
    cancellable = new Cancellable() {
      val delegate = SparkRuntimeUtils.initSparkMain()

      override def cancel(): Unit = {
        stopped = true
        delegate.cancel()
      }
    }

    try {
      val sparkClassLoader = SparkClassLoader.findFromContext()
      val runtimeContext = sparkClassLoader.getRuntimeContext
      val executionContext = sparkClassLoader.getSparkExecutionContext(false)
      val serializableExecutionContext = new SerializableSparkExecutionContext(executionContext)

      // Check one more time before calling user main
      if (stopped) {
        return
      }

      // Load the user Spark class
      val userSparkClass = sparkClassLoader.getProgramClassLoader.loadClass(
        runtimeContext.getSparkSpecification.getMainClassName)
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
          getMainMethod(cls).invoke(null, RuntimeArguments.toPosixArray(runtimeContext.getRuntimeArguments))
      }
      executionContext.waitForSparkHttpService()
    } catch {
      // If it is stopped, ok to ignore the InterruptedException, as system issues interrupt to the main thread
      // to unblock the main method.
      case e: InterruptedException => if (!SparkRuntimeEnv.isStopped) throw e
    } finally {
      cancellable.cancel
    }
  }

  /**
    * Gets the static main method from the givne class.
    */
  private def getMainMethod(obj: Class[_]): Method = {
    try {
      val mainMethod = obj.getDeclaredMethod("main", classOf[Array[String]])
      if (!Modifier.isStatic(mainMethod.getModifiers)) {
        throw new IllegalArgumentException("Static modifiers not used for main method of " + obj.getName)
      }
      mainMethod
    } catch {
      case e: NoSuchMethodException => throw new IllegalArgumentException(obj.getName
        + " is not a supported Spark program. It should either implement "
        + classOf[SparkMain].getName + " or " + classOf[JavaSparkMain].getName
        + " or define a main method")
    }
  }
}
