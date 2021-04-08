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
 *
 */

package io.cdap.cdap.etl.common;

import com.google.gson.Gson;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ConnectionMacroEvaluator implements MacroEvaluator {
  public static final String FUNCTION_NAME = "conn";

  @Override
  public String lookup(String property) throws InvalidMacroException {
    throw new InvalidMacroException("The '" + FUNCTION_NAME
                                      + "' macro function doesn't support direct property lookup for property '"
                                      + property + "'");
  }

  @Override
  public String evaluate(String macroFunction, String... args) throws InvalidMacroException {
    if (!FUNCTION_NAME.equals(macroFunction)) {
      // This shouldn't happen
      throw new IllegalArgumentException("Invalid function name " + macroFunction
                                           + ". Expecting " + FUNCTION_NAME);
    }

    if (args.length != 2) {
      throw new InvalidMacroException("Macro '" + FUNCTION_NAME + "' should have exactly 2 arguments");
    }

    Map<String, String> config = new HashMap<>();
    config.put("project", "yjcdaptest");
    config.put("serviceAccountType", "filePath");
    config.put("serviceFilePath", "/Users/yaojie/Downloads/yjcdaptest-14bfc9d30518.json");
    return new Gson().toJson(config);
  }
}
