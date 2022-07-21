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

package io.cdap.cdap.app.runtime.spark.dynamic

import java.net.URL
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain
import scala.tools.nsc.interpreter.ReplReporter
import scala.tools.nsc.io.AbstractFile

/**
  * A default implementation of [[io.cdap.cdap.api.spark.dynamic.SparkCompiler]] for Scala 2.11 that uses Scala
  * [[scala.tools.nsc.interpreter.IMain]] for the compilation.
  */

class DefaultSparkCompiler(settings: Settings,
                           urlAdder: URLAdder,
                           onClose: () => Unit) extends AbstractSparkCompiler(settings, onClose) {

  override protected def createIMain(settings: Settings, errorReporter: ErrorReporter): IMain with URLAdder = {
    // Overrides the error reporting so that we can collect the errors instead of just getting printed to console
    new IMain(settings) with URLAdder {

      override lazy val reporter: ReplReporter = {
        new ReplReporter(this) {
          // super.reset() refers to different classes in
          // scala 2.12.10 (AbstractReporter) vs 2.12.14 (ReplReporter)
          // Need to use reflection to prevent ClassDefNotFoundError
          lazy val superResetMethod = java.lang.invoke.MethodHandles.lookup().findSpecial(
            classOf[ReplReporter],
            "reset",
            java.lang.invoke.MethodType.methodType(classOf[Unit]),
            this.getClass
          );

          override def printMessage(msg: String): Unit = {
            errorReporter.report(msg)
          }

          override def reset(): Unit = {
            superResetMethod.invoke(this)
            errorReporter.clear()
          }

          override def toString: String = {
            errorReporter.toString
          }
        }
      }

      override def addURLs(urls: URL*): Unit = {
        ensureClassLoader()
        addUrlsToClassPath(urls: _*)
        resetClassLoader()
        urlAdder.addURLs(urls: _*)
      }
    }
  }

  override protected def getOutputDir(): AbstractFile = {
    getIMain().replOutput.dir
  }
}
