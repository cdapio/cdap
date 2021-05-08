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
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.etl.proto.connection.Connection;
import io.cdap.cdap.proto.id.NamespaceId;

import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * A {@link MacroEvaluator} for resolving the {@code ${conn(connection-name)}} macro function. It uses
 * the studio service for getting connection information at runtime.
 */
public class ConnectionMacroEvaluator extends AbstractServiceRetryableMacroEvaluator {
  public static final String FUNCTION_NAME = "conn";
  private static final String SERVICE_NAME = "Connection";

  private final String namespace;
  private final ServiceDiscoverer serviceDiscoverer;
  private final Gson gson;

  public ConnectionMacroEvaluator(String namespace, ServiceDiscoverer serviceDiscoverer) {
    super(FUNCTION_NAME);
    this.namespace = namespace;
    this.serviceDiscoverer = serviceDiscoverer;
    this.gson = new Gson();
  }

  /**
   * Evaluates the connection macro function by calling the Connection service to retrieve the connection information.
   *
   * @param args should contains exactly one arguments. The argument should contain the connection name
   * @return the json representation of the properties of the connection
   */
  @Override
  String evaluateMacro(String macroFunction,
                       String... args) throws InvalidMacroException, IOException, RetryableException {
    if (args.length != 1) {
      throw new InvalidMacroException("Macro '" + FUNCTION_NAME + "' should have exactly 1 arguments");
    }

    HttpURLConnection urlConn = serviceDiscoverer.openConnection(NamespaceId.SYSTEM.getNamespace(),
                                                                 Constants.PIPELINEID,
                                                                 Constants.STUDIO_SERVICE_NAME,
                                                                 String.format("v1/contexts/%s/connections/%s",
                                                                               namespace, args[0]));
    Connection connection = gson.fromJson(validateAndRetrieveContent(SERVICE_NAME, urlConn), Connection.class);
    return gson.toJson(connection.getPlugin().getProperties());
  }
}
