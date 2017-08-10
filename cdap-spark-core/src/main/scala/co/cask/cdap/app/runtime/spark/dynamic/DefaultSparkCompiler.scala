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

package co.cask.cdap.app.runtime.spark.dynamic

import java.io._
import java.net.URL
import java.net.URLClassLoader

import scala.tools.nsc.Global
import scala.tools.nsc.Settings
import scala.tools.nsc.backend.JavaPlatform
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.interpreter.IMain
import scala.tools.nsc.interpreter.ReplGlobal
import scala.tools.nsc.interpreter.ReplReporter
import scala.tools.nsc.io
import scala.tools.nsc.io.AbstractFile
import scala.tools.nsc.reporters.Reporter
import scala.tools.nsc.util.MergedClassPath

/**
  * A default implementation of [[co.cask.cdap.api.spark.dynamic.SparkCompiler]] for Scala 2.10 that uses Scala
  * [[scala.tools.nsc.interpreter.IMain]] for the compilation.
  */
class DefaultSparkCompiler(settings: Settings,
                           outputDir: AbstractFile,
                           urlAdder: URLAdder,
                           onClose: () => Unit) extends AbstractSparkCompiler(settings, onClose) {

  override protected def createIMain(settings: Settings, errorReporter: ErrorReporter): IMain with URLAdder = {
    // Override couple methods so that
    // 1. Compiled classes can be write out to custom directory instead of always in memory
    // 2. Allow adding dependencies to the compiler
    // 3. Collect errors instead of just getting printed to console.
    new IMain(settings) with URLAdder {

      private lazy val runtimeParentClassLoader = new MutableURLClassLoader(Array(), super.parentClassLoader)
      private var runtimeClassLoader: Option[AbstractFileClassLoader] = None

      override protected def parentClassLoader: ClassLoader = {
        runtimeParentClassLoader
      }

      override def classLoader: AbstractFileClassLoader = {
        runtimeClassLoader = runtimeClassLoader.orElse(Some(createClassLoader()))
        runtimeClassLoader.get
      }

      override def resetClassLoader(): Unit = {
        runtimeClassLoader = None
      }

      override protected def newCompiler(settings: Settings, reporter: Reporter): ReplGlobal = {
        settings.outputDirs setSingleOutput outputDir
        settings.exposeEmptyPackage.value = true
        new Global(settings, reporter) with ReplGlobal {
          override def toString: String = "<global>"
        }
      }

      override lazy val reporter: ReplReporter = {
        new ReplReporter(this) {
          override def printMessage(msg: String): Unit = {
            errorReporter.report(msg)
          }

          override def reset(): Unit = {
            super.reset()
            errorReporter.clear()
          }

          override def toString: String = {
            errorReporter.toString
          }
        }
      }

      override def addURLs(urls: URL*): Unit = {
        updateCompilerClassPath(urls: _*)
        urls.foreach(runtimeParentClassLoader.addURL)
        urlAdder.addURLs(urls: _*)
      }

      /**
        * Updates the classpath used by the Scala compiler.
        */
      private def updateCompilerClassPath(urls: URL*): Unit = {
        require(global.platform.isInstanceOf[JavaPlatform], "Only JavaPlatform is supported")

        val platform = global.platform.asInstanceOf[JavaPlatform]

        val newClassPath = mergeUrlsIntoClassPath(platform, urls: _*)

        // NOTE: Must use reflection until this is exposed/fixed upstream in Scala
        val fieldSetter = platform.getClass.getMethods
          .find(_.getName.endsWith("currentClassPath_$eq")).get
        fieldSetter.invoke(platform, Some(newClassPath))

        // Reload all jars specified into our compiler
        global.invalidateClassPathEntries(urls.map(_.getPath): _*)
      }

      /**
        * Creates a new [[scala.tools.nsc.util.MergedClassPath]] by merging the existing classpath
        * used by the platform with the given set of URLs.
        */
      private def mergeUrlsIntoClassPath(platform: JavaPlatform, urls: URL*): MergedClassPath[AbstractFile] = {
        // Collect our new jars/directories and add them to the existing set of classpaths
        val allClassPaths = (
          platform.classPath.asInstanceOf[MergedClassPath[AbstractFile]].entries ++
            urls.map(url => {
              platform.classPath.context.newClassPath(
                if (url.getProtocol == "file") {
                  val f = new File(url.getPath)
                  if (f.isDirectory)
                    io.AbstractFile.getDirectory(f)
                  else
                    io.AbstractFile.getFile(f)
                } else {
                  io.AbstractFile.getURL(url)
                }
              )
            })
          ).distinct

        // Combine all of our classpaths (old and new) into one merged classpath
        new MergedClassPath(allClassPaths, platform.classPath.context)
      }

      /**
        * Creates a new classloader used for runtime classloading.
        */
      private def createClassLoader(): AbstractFileClassLoader = {
        // This is copied from Scala IMain in order to use a different AbstractFile instead of the virtualDirectory.
        new AbstractFileClassLoader(outputDir, parentClassLoader) {
          /** Overridden here to try translating a simple name to the generated
            *  class name if the original attempt fails.  This method is used by
            *  getResourceAsStream as well as findClass.
            */
          override protected def findAbstractFile(name: String): AbstractFile = {
            super.findAbstractFile(name) match {
              // deadlocks on startup if we try to translate names too early
              case null if isInitializeComplete => generatedName(name) map (x => super.findAbstractFile(x)) orNull
              case file => file
            }
          }
        }
      }
    }
  }

  override protected def getOutputDir(): AbstractFile = {
    outputDir
  }

  /**
    * A [[java.net.URLClassLoader]] that exposes the addURL method.
    */
  private class MutableURLClassLoader(urls: Array[URL], parent: ClassLoader) extends URLClassLoader(urls, parent) {

    override def addURL(url: URL): Unit = {
      super.addURL(url)
    }
  }
}
