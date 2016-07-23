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
import co.cask.cdap.api.macro.MacroFunction;
import co.cask.cdap.api.macro.Macros;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A macro evaluator used strictly for collecting macro properties and functions.
 *
 * The evaluator is passed as an argument to a {@link MacroParser} and internally keeps
 * track of macro properties and macro functions
 * TODO: CDAP-6628 this currently works only for simple properties lookup and simple functions,
 * nested macros wouldn't work and needs change in macroparser logic
 * TODO: consolidate this with TrackingMacroEvaluator
 */
public class CollectMacroEvaluator implements MacroEvaluator {
  private final Set<String> lookupProperties;
  private final Set<MacroFunction> macroFunctions;

  public CollectMacroEvaluator() {
    this.lookupProperties = new HashSet<>();
    this.macroFunctions = new HashSet<>();
  }

  public String lookup(String property) {
    lookupProperties.add(property);
    return "";
  }

  public String evaluate(String macroFunction, String... arguments) {
    List<String> argumentsList = new ArrayList<>();
    for (String arg : arguments) {
      argumentsList.add(arg);
    }
    macroFunctions.add(new MacroFunction(macroFunction, argumentsList));
    return "";
  }

  public Macros getMacros() {
    return new Macros(lookupProperties, macroFunctions);
  }
}
