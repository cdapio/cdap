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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.macro.InvalidMacroException;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.common.macro.LogicalStartTimeMacro;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Macro evaluator used by batch application
 */
public class DefaultMacroEvaluator implements MacroEvaluator {
  private final Map<String, String> resolvedArguments;
  private final long logicalStartTime;
  private final LogicalStartTimeMacro logicalStartTimeMacro;
  private final SecureStore secureStore;
  private final String namespace;

  private static final String LOGICAL_START_TIME_FUNCTION_NAME = "logicalStartTime";
  private static final String SECURE_FUNCTION_NAME = "secure";

  public DefaultMacroEvaluator(@Nullable WorkflowToken workflowToken, Map<String, String> runtimeArguments,
                               long logicalStartTime, SecureStore secureStore, String namespace) {
    Map<String, String> resolvedArguments = new HashMap<>();
    resolvedArguments.putAll(runtimeArguments);
    // not expected, but can happen if user runs just the program and not the workflow
    if (workflowToken != null) {
      for (String tokenKey : workflowToken.getAll(WorkflowToken.Scope.USER).keySet()) {
        resolvedArguments.put(tokenKey, workflowToken.get(tokenKey, WorkflowToken.Scope.USER).toString());
      }
    }
    this.resolvedArguments = resolvedArguments;
    this.logicalStartTime = logicalStartTime;
    this.logicalStartTimeMacro = new LogicalStartTimeMacro();
    this.secureStore = secureStore;
    this.namespace = namespace;
  }

  public DefaultMacroEvaluator(Map<String, String> arguments, long logicalStartTime, SecureStore secureStore,
                               String namespace) {
    this.resolvedArguments = arguments;
    this.logicalStartTime = logicalStartTime;
    this.logicalStartTimeMacro = new LogicalStartTimeMacro();
    this.secureStore = secureStore;
    this.namespace = namespace;
  }

  @Override
  @Nullable
  public String lookup(String property) {
    String val = resolvedArguments.get(property);
    if (val == null) {
      throw new InvalidMacroException(String.format("Argument '%s' is not defined.", property));
    }
    return val;
  }

  @Override
  @Nullable
  public String evaluate(String macroFunction, String... arguments) throws InvalidMacroException {
    if (macroFunction.equals(LOGICAL_START_TIME_FUNCTION_NAME)) {
      return logicalStartTimeMacro.evaluate(logicalStartTime, arguments);
    } else if (macroFunction.equals(SECURE_FUNCTION_NAME)) {
      if (arguments.length != 1) {
        throw new InvalidMacroException("Secure store macro function only supports 1 argument.");
      }
      try {
        return Bytes.toString(secureStore.getSecureData(namespace, arguments[0]).get());
      } catch (Exception e) {
        throw new InvalidMacroException("Failed to resolve macro '" + macroFunction + "(" + arguments[0] + ")'", e);
      }
    }
    throw new InvalidMacroException(String.format("Unsupported macro function %s", macroFunction));
  }
}
