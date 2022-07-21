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

import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.etl.proto.connection.ConnectionId;

import java.util.HashSet;
import java.util.Set;

/**
 * Connection macro evaluator to register connection usage. It shouldn't be used if for real macro evaluation
 */
public class ConnectionRegistryMacroEvaluator implements MacroEvaluator {
  public static final String FUNCTION_NAME = "conn";

  private final Set<String> connectionNames;

  public ConnectionRegistryMacroEvaluator() {
    this.connectionNames = new HashSet<>();
  }

  @Override
  public String lookup(String property) throws InvalidMacroException {
    throw new InvalidMacroException("The '" + FUNCTION_NAME
                                      + "' macro function doesn't support direct property lookup for property '"
                                      + property + "'");
  }

  @Override
  public String evaluate(String macroFunction, String... args) throws InvalidMacroException {
    if (args.length != 1) {
      throw new InvalidMacroException("Macro '" + FUNCTION_NAME + "' should have exactly 1 arguments");
    }

    connectionNames.add(ConnectionId.getConnectionId(args[0]));

    throw new InvalidMacroException("The '" + FUNCTION_NAME
                                      + "' macro function doesn't support evaluating the connection macro " +
                                      "for connection '" + args[0] + "'");
  }

  public Set<String> getUsedConnections() {
    return connectionNames;
  }
}
