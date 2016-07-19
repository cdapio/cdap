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

package co.cask.cdap.internal.app.runtime.plugin;

import co.cask.cdap.api.macro.MacroEvaluator;

/**
 * A macro evaluator used strictly for checking if strings contains valid macros.
 *
 * The evaluator is passed as an argument to a {@link MacroParser} and internally keeps
 * track of whether or not a macro was found when the parser
 */
public class TrackingMacroEvaluator implements MacroEvaluator {
  private boolean foundMacro;

  public TrackingMacroEvaluator() {
    this.foundMacro = false;
  }

  public String lookup(String property) {
    foundMacro = true;
    return "";
  }

  public String evaluate(String macroFunction, String... arguments) {
    foundMacro = true;
    return "";
  }

  /**
   * Returns whether or not the last String parsed by the evaluator's associated
   * {@link MacroParser} contained a macro.
   * @return if the evaluator found a macro in the last String parsed.
   */
  public boolean hasMacro() {
    return foundMacro;
  }

  /**
   * Resets whether the evaluator found a macro in the last String parsed to false.
   */
  public void reset() {
    foundMacro = false;
  }
}
