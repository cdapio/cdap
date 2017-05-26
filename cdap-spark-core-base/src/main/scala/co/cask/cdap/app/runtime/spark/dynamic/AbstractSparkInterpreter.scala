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

import co.cask.cdap.api.spark.dynamic.{BindingException, InterpretFailureException, SparkInterpreter}

import scala.reflect.{ClassTag, runtime}
import scala.tools.nsc.interpreter.Results.{Error, Incomplete, Success}

/**
  * A trait to provide implementation of [[co.cask.cdap.api.spark.dynamic.SparkInterpreter]] that uses
  * [[scala.tools.nsc.interpreter.IMain]] exposed by [[co.cask.cdap.api.spark.dynamic.SparkCompiler]].
  */
trait AbstractSparkInterpreter extends SparkInterpreter {

  override def bind[T: runtime.universe.TypeTag : ClassTag](name: String, value: T): Unit = {
    val valueType = implicitly[runtime.universe.TypeTag[T]].tpe
    val iMain = getIMain()
    iMain.reporter.reset()
    iMain.bind(name, value) match {
      case Error | Incomplete =>
        throw new BindingException(name, valueType.toString(), value)
      case Success =>
    }
  }

  override def bind(name: String, bindType: String, value: Any, modifiers: String*): Unit = {
    val iMain = getIMain()

    iMain.reporter.reset()
    iMain.bind(name, bindType, value, modifiers.toList) match {
      case Error | Incomplete =>
        throw new BindingException(name, bindType, value, modifiers: _*)
      case Success =>
    }
  }

  override def interpret(line: String): Unit = {
    getIMain().interpret(line) match {
      case Error => throw new InterpretFailureException(getIMain().reporter.toString)
      case Incomplete => throw new InterpretFailureException("Source line is incomplete: " + line)
      case Success =>
    }
  }

  override def getValue(name: String): Option[Any] = {
    getIMain().valueOfTerm(name)
  }
}
