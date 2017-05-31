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

package co.cask.cdap.api.spark.dynamic

import java.io.{File, IOException, OutputStream}

import co.cask.cdap.api.annotation.Beta

import scala.tools.nsc.interpreter.IMain

/**
  * A compiler for compiling Spark code in Scala
  */
@Beta
trait SparkCompiler extends AutoCloseable {

  /**
    * Adds dependency to the compiler. Classes loadable from the given file are usable by the code to be compiled
    * by this compiler.
    *
    * @param file a directory or a jar file.
    */
  def addDependency(file: File): Unit

  /**
    * Compiles the given source code. The source code must have content the same as a valid scala source file.
    *
    * @param source the source string to compile
    * @throws co.cask.cdap.api.spark.dynamic.CompilationFailureException if compilation failed
    */
  @throws(classOf[CompilationFailureException])
  def compile(source: String): Unit

  /**
    * Compiles the given scala source file.
    *
    * @param file the file to compile
    * @throws co.cask.cdap.api.spark.dynamic.CompilationFailureException if compilation failed
    */
  @throws(classOf[CompilationFailureException])
  def compile(file: File): Unit

  /**
    * Saves all compiled classes to a JAR file.
    *
    * @param file the location of the JAR file
    * @throws java.io.IOException if failed to save to the JAR file
    */
  @throws(classOf[IOException])
  def saveAsJar(file: File): Unit

  /**
    * Saves all compiled classes to a JAR.
    *
    * @param outputStream the [[java.io.OutputStream]] for writing out the JAR content
    * @throws java.io.IOException if failed to write out the JAR
    */
  @throws(classOf[IOException])
  def saveAsJar(outputStream: OutputStream): Unit

  /**
    * Releases resources used by the compiler.
    */
  override def close(): Unit

  /**
    * Gets the underlying Scala [[scala.tools.nsc.interpreter.IMain]] for advanced control over code compilation
    * and interpretation
    *
    * @return a [[scala.tools.nsc.interpreter.IMain]] instance
    */
  def getIMain(): IMain
}
