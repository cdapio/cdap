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
import java.util.jar.{JarEntry, JarOutputStream}

import co.cask.cdap.api.spark.dynamic.{CompilationFailureException, SparkCompiler}
import co.cask.cdap.common.lang.ClassLoaders
import co.cask.cdap.common.lang.jar.BundleJarUtil

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.internal.util.{BatchSourceFile, SourceFile}
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain
import scala.tools.nsc.io.{AbstractFile, PlainFile}

/**
  * Abstract common base for implementation [[co.cask.cdap.api.spark.dynamic.SparkCompiler]] for different
  * Spark version.
  */
abstract class AbstractSparkCompiler(settings: Settings, onClose: () => Unit) extends SparkCompiler {

  import AbstractSparkCompiler._

  private val errorReporter = new ErrorReporter()
  private lazy val iMain = createIMain(settings, errorReporter)
  private var compileCount = 0

  /**
    * Creates a new instance of [[scala.tools.nsc.interpreter.IMain]].
    *
    * @param settings the settings for the IMain
    * @param errorReporter a [[co.cask.cdap.app.runtime.spark.dynamic.AbstractSparkCompiler.ErrorReporter]] for
    *                      error reporting from the IMain
    * @return a new instance of IMain
    */
  protected def createIMain(settings: Settings, errorReporter: ErrorReporter): IMain with URLAdder

  /**
    * Returns the directory where the compiler writes out compiled class files.
    *
    * @return a [[scala.tools.nsc.io.AbstractFile]] represeting output directory
    */
  protected def getOutputDir(): AbstractFile

  override def compile(source: String): Unit = {
    compileCount += 1
    compile(new BatchSourceFile(s"<source-string-$compileCount>", source));
  }

  override def compile(file: File): Unit = {
    compile(new BatchSourceFile(new PlainFile(file)))
  }

  override def saveAsJar(file: File): Unit = {
    val output = new BufferedOutputStream(new FileOutputStream(file))
    try {
      saveAsJar(output)
    } finally {
      output.close()
    }
  }

  override def saveAsJar(outputStream: OutputStream): Unit = {
    val outputDir = getOutputDir()
    val jarOutput = new JarOutputStream(outputStream)
    try {
      // If it is file, use the BundleJarUtil to handle it
      if (outputDir.isInstanceOf[PlainFile]) {
        BundleJarUtil.addToArchive(outputDir.file, jarOutput)
        return
      }

      // Otherwise, iterate the AbstractFile recursively and create the jar
      val queue = new mutable.Queue[AbstractFile]()
      outputDir.foreach(f => queue.enqueue(f))
      while (queue.nonEmpty) {
        val entry = queue.dequeue()
        val name = entry.path.substring(outputDir.path.length + 1)
        if (entry.isDirectory) {
          // If it is directory, add the entry with name ended with "/". Also add all children entries to the queue.
          jarOutput.putNextEntry(new JarEntry(name + "/"))
          jarOutput.closeEntry()
          entry.foreach(f => queue.enqueue(f))
        } else {
          jarOutput.putNextEntry(new JarEntry(name))
          copyStreams(entry.input, jarOutput)
          jarOutput.closeEntry()
        }
      }
    } finally {
      jarOutput.close()
    }
  }

  override def addDependency(file: File): Unit = {
    iMain.addURLs(file.toURI.toURL)
  }

  override def getIMain(): IMain = {
    return iMain
  }

  override def close(): Unit = {
    iMain.reset()
    iMain.close()
    onClose()
  }

  /**
    * Compiles using content defined by the given [[scala.reflect.internal.util.SourceFile]].
    */
  private def compile(sourceFile: SourceFile): Unit = {
    if (!iMain.compileSources(sourceFile)) {
      throw new CompilationFailureException(errorReporter.toString)
    }
  }

  /**
    * Class for reporting errors generated from [[scala.tools.nsc.interpreter.IMain]].
    */
  protected final class ErrorReporter {

    private val errors = new mutable.ListBuffer[String]

    def report(msg: String): Unit = {
      errors += msg
    }

    def clear(): Unit = {
      errors.clear()
    }

    override def toString: String = {
      errors.mkString(System.getProperty("line.separator"))
    }
  }
}

/**
  * Companion object to provide helper methods.
  */
object AbstractSparkCompiler {

  /**
    * Setup the [[scala.tools.nsc.settings]] user classpath based on the give [[java.lang.ClassLoader]].
    *
    * @param settings the settings to modify
    * @param classLoader the classloader to use to determine the set of URLs to generate the user classpath
    * @return the same settings instance from the argument
    */
  def setClassPath(settings: Settings, classLoader: ClassLoader): Settings = {
    settings.classpath.value =
      ClassLoaders.getClassLoaderURLs(classLoader, true, new java.util.LinkedHashSet[URL])
                  .mkString(File.pathSeparator)
    settings
  }

  /**
    * Copying data from one stream to another.
    */
  private def copyStreams(from: InputStream, to: OutputStream): Unit = {
    val buf = new Array[Byte](8192)
    var len = from.read(buf)
    while (len >= 0) {
      to.write(buf, 0, len)
      len = from.read(buf)
    }
  }
}