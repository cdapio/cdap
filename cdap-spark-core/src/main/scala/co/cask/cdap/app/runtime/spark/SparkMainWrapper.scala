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

import java.lang.reflect.{Method, Modifier}

import co.cask.cdap.api.common.RuntimeArguments
import co.cask.cdap.api.spark.{JavaSparkMain, SparkMain}

/**
  * The main class that get submitted to Spark for execution of Spark program in CDAP.
  * The first command line argument to this class is the name of the user's Spark program class.
  */
object SparkMainWrapper {

  def main(args: Array[String]):Unit = {
    require(args.length >= 1, "Missing SparkMain class name")

    // Find the SparkRuntimeContext from the classloader
    val sparkClassLoader = SparkClassLoader.findFromContext
    val runtimeContext = sparkClassLoader.getRuntimeContext

    // Load the user Spark class
    val userSparkClass = sparkClassLoader.getProgramClassLoader.loadClass(args(0))
    val executionContext = sparkClassLoader.createExecutionContext()
    try {
      userSparkClass match {
        // SparkMain
        case cls if classOf[SparkMain].isAssignableFrom(cls) =>
          cls.asSubclass(classOf[SparkMain]).newInstance().run(executionContext)

        // JavaSparkMain
        case cls if classOf[JavaSparkMain].isAssignableFrom(cls) =>
          cls.asSubclass(classOf[JavaSparkMain]).newInstance().run(
            sparkClassLoader.createJavaExecutionContext(executionContext))

        // main() method
        case cls =>
          getMainMethod(cls).fold(
            throw new IllegalArgumentException(userSparkClass.getName
              + " is not a supported Spark program. It should implements either "
              + classOf[SparkMain].getName + " or " + classOf[JavaSparkMain].getName)
          )(
            _.invoke(null, RuntimeArguments.toPosixArray(runtimeContext.getRuntimeArguments))
          )
      }
    } finally {
      executionContext match {
        case c: AutoCloseable => c.close()
        case _ => // no-op
      }
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
}
