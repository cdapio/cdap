/*
 * Copyright © 2021 Cask Data, Inc.
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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.security.store.SecureStore;

/**
 * Evaluates secure store macros if the key exists. Otherwise, returns the input.
 */
public class SecureStoreMacroEvaluator implements MacroEvaluator {

  public static final String FUNCTION_NAME = "secure";

  private final String namespace;
  private final SecureStore secureStore;

  public SecureStoreMacroEvaluator(String namespace, SecureStore secureStore) {
    this.namespace = namespace;
    this.secureStore = secureStore;
  }

  @Override
  public String lookup(String property) throws InvalidMacroException {
    // this will get ignored by the parser
    throw new InvalidMacroException("Unable to lookup the value for " + property);
  }

  @Override
  public String evaluate(String macroFunction, String... args) throws InvalidMacroException {
    if (!FUNCTION_NAME.equals(macroFunction)) {
      // This shouldn't happen
      throw new IllegalArgumentException("Invalid function name " + macroFunction
                                           + ". Expecting " + FUNCTION_NAME);
    }
    if (args.length != 1) {
      throw new InvalidMacroException("Macro '" + FUNCTION_NAME + "' should have exactly 1 argument");
    }
    try {
      return Bytes.toString(secureStore.get(namespace, args[0]).get());
    } catch (Exception e) {
      throw new InvalidMacroException("Failed to resolve macro '" + FUNCTION_NAME + "(" + args[0] + ")'", e);
    }
  }
}
