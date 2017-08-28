/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark.python;

import py4j.GatewayServer;
import py4j.Py4JNetworkException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Abstract base utility class for PySpark. Different Spark version has different implementation, due to API changes
 * in Spark.
 */
public abstract class AbstractSparkPythonUtil {

  /**
   * Starts a Py4j gateway server.
   *
   * @param portFile the file to have the gateway server listening port (in string format) written to
   * @return the gateway server
   * @throws IOException if failed to start the server or failed to write out the port.
   */
  public static GatewayServer startPy4jGateway(Path portFile) throws IOException {
    GatewayServer server = new GatewayServer(null, 0);
    try {
      server.start();
    } catch (Py4JNetworkException e) {
      throw new IOException(e);
    }

    // Write the port number in string form to the port file
    Files.write(portFile, Integer.toString(server.getListeningPort()).getBytes(StandardCharsets.UTF_8));
    return server;
  }
}
