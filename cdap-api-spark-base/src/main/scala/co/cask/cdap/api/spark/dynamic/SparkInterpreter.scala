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

import co.cask.cdap.api.annotation.Beta

import scala.reflect.{ClassTag, runtime}

/**
  * An interpreter for running Spark code in Scala.
  */
@Beta
trait SparkInterpreter extends SparkCompiler {

  /**
    * Binds a variable to a specific value.
    *
    * @param name name of the variable
    * @param value value to bind to
    * @tparam T type of the variable, derived from the value type
    * @throws co.cask.cdap.api.spark.dynamic.BindingException if failed to bind the variable
    */
  @throws(classOf[BindingException])
  def bind[T: runtime.universe.TypeTag : ClassTag](name: String, value: T): Unit

  /**
    * Binds a variable of a specific type to a specific value.
    *
    * @param name name of the variable
    * @param bindType type of the variable
    * @param value value to bind to
    * @param modifiers a list of modifies for the variable, such as `private`, `implicit`, etc.
    * @throws co.cask.cdap.api.spark.dynamic.BindingException if failed to bind the variable
    */
  @throws(classOf[BindingException])
  def bind(name: String, bindType: String, value: Any, modifiers: String*): Unit

  /**
    * Interprets a line of source code.
    *
    * @param line the source line to interpret
    * @throws co.cask.cdap.api.spark.dynamic.InterpretFailureException if the interpretation failed
    */
  @throws(classOf[InterpretFailureException])
  def interpret(line: String): Unit

  /**
    * Gets the value of the given variable if it exists in the context.
    *
    * @param name name of the variable
    * @return an [[scala.Option]] that will content [[scala.Some]] value if it exists, or [[scala.None]] otherwise.
    */
  def getValue(name: String): Option[Any]
}
