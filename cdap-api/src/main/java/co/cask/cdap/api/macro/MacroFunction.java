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

import java.util.List;

/**
 * Class representing a macro function
 */
public class MacroFunction {
  private final String functionName;
  private final List<String> arguments;

  public MacroFunction(String functionName, List<String> arguments) {
    this.functionName = functionName;
    this.arguments = arguments;
  }

  /**
   * return the function name
   * @return funtion name
   */
  public String getFunctionName() {
    return functionName;
  }

  /**
   * return the list of arguments to the funcion
   * @return the list of arguments
   */
  public List<String> getArguments() {
    return arguments;
  }
}
