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
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.etl.common.macro.LogicalStartTimeMacroEvaluator;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Macro evaluator used by batch application
 */
public class DefaultMacroEvaluator implements MacroEvaluator {

  private final BasicArguments arguments;
  private final Map<String, MacroEvaluator> macroEvaluators;

  public DefaultMacroEvaluator(BasicArguments arguments, long logicalStartTime,
                               SecureStore secureStore, ServiceDiscoverer serviceDiscoverer, String namespace) {
    this(arguments, ImmutableMap.of(
      LogicalStartTimeMacroEvaluator.FUNCTION_NAME, new LogicalStartTimeMacroEvaluator(logicalStartTime),
      SecureStoreMacroEvaluator.FUNCTION_NAME, new SecureStoreMacroEvaluator(namespace, secureStore),
      OAuthMacroEvaluator.FUNCTION_NAME, new OAuthMacroEvaluator(serviceDiscoverer)
    ));
  }

  public DefaultMacroEvaluator(BasicArguments arguments, Map<String, MacroEvaluator> macroEvaluators) {
    this.arguments = arguments;
    this.macroEvaluators = ImmutableMap.copyOf(macroEvaluators);
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
    MacroEvaluator evaluator = macroEvaluators.get(macroFunction);
    if (evaluator == null) {
      throw new InvalidMacroException(String.format("Unsupported macro function %s", macroFunction));
    }

    return evaluator.evaluate(macroFunction, args);
  }
}
