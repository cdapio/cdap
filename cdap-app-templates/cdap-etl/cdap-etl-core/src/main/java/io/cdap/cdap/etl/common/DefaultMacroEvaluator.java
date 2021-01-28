/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.etl.common.macro.LogicalStartTimeMacro;

import javax.annotation.Nullable;

/**
 * Macro evaluator used by batch application
 */
public class DefaultMacroEvaluator implements MacroEvaluator {
  private final BasicArguments arguments;
  private final long logicalStartTime;
  private final LogicalStartTimeMacro logicalStartTimeMacro;
  private final SecureStore secureStore;
  private final ServiceDiscoverer serviceDiscoverer;
  private final String namespace;

  private static final String LOGICAL_START_TIME_FUNCTION_NAME = "logicalStartTime";
  private static final String SECURE_FUNCTION_NAME = "secure";

  public DefaultMacroEvaluator(BasicArguments arguments, long logicalStartTime,
                               SecureStore secureStore, ServiceDiscoverer serviceDiscoverer, String namespace) {
    this.arguments = arguments;
    this.logicalStartTime = logicalStartTime;
    this.logicalStartTimeMacro = new LogicalStartTimeMacro();
    this.secureStore = secureStore;
    this.serviceDiscoverer = serviceDiscoverer;
    this.namespace = namespace;
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
  public String evaluate(String macroFunction, String... arguments) throws InvalidMacroException {
    if (macroFunction.equals(LOGICAL_START_TIME_FUNCTION_NAME)) {
      return logicalStartTimeMacro.evaluate(logicalStartTime, arguments);
    } else if (macroFunction.equals(SECURE_FUNCTION_NAME)) {
      if (arguments.length != 1) {
        throw new InvalidMacroException("Secure store macro function only supports 1 argument.");
      }
      try {
        return Bytes.toString(secureStore.get(namespace, arguments[0]).get());
      } catch (Exception e) {
        throw new InvalidMacroException("Failed to resolve macro '" + macroFunction + "(" + arguments[0] + ")'", e);
      }
    }
    throw new InvalidMacroException(String.format("Unsupported macro function %s", macroFunction));
  }
}
