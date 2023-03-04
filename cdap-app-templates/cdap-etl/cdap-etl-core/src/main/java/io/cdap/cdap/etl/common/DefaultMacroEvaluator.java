/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroObjectType;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.etl.common.macro.LogicalStartTimeMacroEvaluator;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Macro evaluator used by batch application
 */
public class DefaultMacroEvaluator implements MacroEvaluator {

  public static final Set<String> MAP_FUNCTIONS =
      ImmutableSet.of(ConnectionMacroEvaluator.FUNCTION_NAME, OAuthMacroEvaluator.FUNCTION_NAME);

  private final BasicArguments arguments;
  private final Map<String, MacroEvaluator> macroEvaluators;
  private final Set<String> mapFunctions;

  public DefaultMacroEvaluator(BasicArguments arguments, long logicalStartTime,
      SecureStore secureStore, ServiceDiscoverer serviceDiscoverer, String namespace) {
    this(arguments, ImmutableMap.of(
        LogicalStartTimeMacroEvaluator.FUNCTION_NAME,
        new LogicalStartTimeMacroEvaluator(logicalStartTime),
        SecureStoreMacroEvaluator.FUNCTION_NAME,
        new SecureStoreMacroEvaluator(namespace, secureStore),
        OAuthMacroEvaluator.FUNCTION_NAME, new OAuthMacroEvaluator(serviceDiscoverer),
        OAuthAccessTokenMacroEvaluator.FUNCTION_NAME,
        new OAuthAccessTokenMacroEvaluator(serviceDiscoverer),
        ConnectionMacroEvaluator.FUNCTION_NAME,
        new ConnectionMacroEvaluator(namespace, serviceDiscoverer)
    ), MAP_FUNCTIONS);
  }

  public DefaultMacroEvaluator(BasicArguments arguments,
      Map<String, MacroEvaluator> macroEvaluators,
      Set<String> mapFunctions) {
    this.arguments = arguments;
    this.macroEvaluators = ImmutableMap.copyOf(macroEvaluators);
    if (!macroEvaluators.keySet().containsAll(mapFunctions)) {
      throw new IllegalArgumentException(
          String.format("The given macro evaluators %s should contain all map functions %s",
              macroEvaluators.keySet(), mapFunctions));
    }
    this.mapFunctions = mapFunctions;
  }

  @Override
  @Nullable
  public String lookup(String property) {
    String val = arguments.get(property);
    if (val == null) {
      throw new InvalidMacroException(String.format("Argument '%s' is not defined.", property));
    }
    return val;
  }

  @Override
  @Nullable
  public String evaluate(String macroFunction, String... args) throws InvalidMacroException {
    MacroEvaluator evaluator = getMacroEvaluator(macroFunction);
    return evaluator.evaluate(macroFunction, args);
  }

  @Override
  public Map<String, String> evaluateMap(String macroFunction, String... arguments)
      throws InvalidMacroException {
    MacroEvaluator evaluator = getMacroEvaluator(macroFunction);
    if (!mapFunctions.contains(macroFunction)) {
      throw new InvalidMacroException(
          String.format("The macro function %s cannot be evaluated as map. "
              + "Please use evaluate() instead", macroFunction));
    }
    return evaluator.evaluateMap(macroFunction, arguments);
  }

  @Override
  public MacroObjectType evaluateAs(String macroFunction) {
    return mapFunctions.contains(macroFunction) ? MacroObjectType.MAP : MacroObjectType.STRING;
  }

  private MacroEvaluator getMacroEvaluator(String macroFunction) {
    MacroEvaluator evaluator = macroEvaluators.get(macroFunction);
    if (evaluator == null) {
      throw new InvalidMacroException(
          String.format("Unsupported macro function %s", macroFunction));
    }
    return evaluator;
  }
}
