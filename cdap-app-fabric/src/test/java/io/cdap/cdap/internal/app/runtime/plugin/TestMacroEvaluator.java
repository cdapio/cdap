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

package io.cdap.cdap.internal.app.runtime.plugin;

import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroObjectType;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Test macro evaluator that supports lookups and test(key) and t(key) functions.
 */
public class TestMacroEvaluator implements MacroEvaluator {
  private static final Set<String> MAP_FUNCTIONS = Collections.singleton("testmap");

  private final Map<String, String> propertySubstitutions;
  private final Map<String, String> macroFunctionSubstitutions;
  private final Map<String, Map<String, String>> mapFunctionSubstitutions;
  private final boolean failIfEvaluated;

  public TestMacroEvaluator(Map<String, String> propertySubstitutions, Map<String, String> macroFunctionSubstitutions) {
    this(propertySubstitutions, macroFunctionSubstitutions, false);
  }

  public TestMacroEvaluator(Map<String, String> propertySubstitutions, Map<String, String> macroFunctionSubstitutions,
                            boolean failIfEvaluated) {
    this(propertySubstitutions, macroFunctionSubstitutions, failIfEvaluated, Collections.emptyMap());
  }

  public TestMacroEvaluator(Map<String, String> propertySubstitutions, Map<String, String> macroFunctionSubstitutions,
                            boolean failIfEvaluated, Map<String, Map<String, String>> mapFunctionSubstitutions) {
    this.propertySubstitutions = propertySubstitutions;
    this.macroFunctionSubstitutions = macroFunctionSubstitutions;
    this.failIfEvaluated = failIfEvaluated;
    this.mapFunctionSubstitutions = mapFunctionSubstitutions;
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
    if (failIfEvaluated) {
      throw new RuntimeException("Macro function should not get evaluated");
    }

    if (!macroFunction.equals("test") && !macroFunction.equals("t")) {
      throw new InvalidMacroException(String.format("Macro function '%s' not defined.", macroFunction));
    }
    if (arguments.length > 1) {
      throw new InvalidMacroException("Test macro function only takes 1 argument.");
    }
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

  @Override
  public Map<String, String> evaluateMap(String macroFunction, String... arguments) throws InvalidMacroException {
    if (!macroFunction.equals("testmap")) {
      throw new InvalidMacroException(String.format("Macro function '%s' not defined.", macroFunction));
    }

    if (arguments.length > 1) {
      throw new InvalidMacroException("Test macro function only takes 1 argument.");
    }
    String value = arguments[0];
    if (!mapFunctionSubstitutions.containsKey(value)) {
      throw new InvalidMacroException(String.format("Macro '%s' not specified.", value));

    }
    return mapFunctionSubstitutions.get(value);
  }

  @Override
  public MacroObjectType evaluateAs(String macroFunction) {
    return MAP_FUNCTIONS.contains(macroFunction) ? MacroObjectType.MAP : MacroObjectType.STRING;
  }
}
