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

import co.cask.cdap.api.macro.InvalidMacroException;
import co.cask.cdap.api.macro.MacroEvaluator;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class TestMacroEvaluator implements MacroEvaluator {

  private final Map<String, String> propertySubstitutions;
  private final Map<String, String> macroFunctionSubstitutions;

  public TestMacroEvaluator(Map<String, String> propertySubstitutions, Map<String, String> macroFunctionSubstitutions) {
    this.propertySubstitutions = propertySubstitutions;
    this.macroFunctionSubstitutions = macroFunctionSubstitutions;
  }

  @Override
  public String lookup(String value)  {
    if (value == null) {
      value = "";
    }
    String substitution = propertySubstitutions.get(value);
    if (substitution == null) {
      throw new InvalidMacroException(String.format("Macro '%s' not specified.", value));
    }
    return substitution;
  }

  @Override
  public String evaluate(String macroFunction, String... arguments) {
    if (!macroFunction.equals("test")) {
      throw new InvalidMacroException(String.format("Macro function '%s' not defined.", macroFunction));
    } else if (arguments.length > 1) {
      throw new InvalidMacroException("Test macro function only takes 1 argument.");
    } else {
      String value = arguments[0];
      if (value == null) {
        value = "";
      }
      String substitution = macroFunctionSubstitutions.get(value);
      if (substitution == null) {
        throw new InvalidMacroException(String.format("Macro '%s' not specified.", value));
      }
      return substitution;
    }
  }
}
