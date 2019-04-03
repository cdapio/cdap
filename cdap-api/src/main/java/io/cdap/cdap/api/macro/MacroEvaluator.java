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

package co.cask.cdap.api.macro;

import co.cask.cdap.api.annotation.Beta;

/**
 * Macro evaluator helps to provide macro substitution at runtime.
 *
 * MacroEvaluator supports two kinds of macros 1) property lookup  2) macro functions.
 * property lookup:
 * Syntax : ${macro}
 * Description: lookup the key "macro" from properties and return the value for the key, could return null if not found.
 * Example : ${user-name}
 *
 * macro functions:
 * Syntax : ${macroFunction(macro)}
 * Description: call the macroFunction with "macro" as the argument to the function.
 * Example : ${secure(accessKey)} - macro function "secure" is called with argument "accessKey".
 */

@Beta
public interface MacroEvaluator {

  /**
   * lookup the property and return the value corresponding to the property.
   * @param property name of the property to lookup
   * @return looked up value, could be null if property is not found
   * @throws InvalidMacroException if property evaluates invalid macro
   */
  String lookup(String property) throws InvalidMacroException;

  /**
   * Use the macro function and call the function with provided arguments,
   * function uses the arguments and returns the evaluated response.
   * @param macroFunction macro function that has to be called
   * @param arguments arguments that will be passed to the macro function
   * @return value returned by macro function
   * @throws InvalidMacroException if macroFunction is not supported
   */
  String evaluate(String macroFunction, String... arguments) throws InvalidMacroException;
}
