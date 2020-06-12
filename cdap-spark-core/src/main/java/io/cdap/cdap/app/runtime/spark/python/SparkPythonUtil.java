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

package io.cdap.cdap.app.runtime.spark.python;

import io.cdap.cdap.app.runtime.spark.SparkRuntimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.CallbackClient;
import py4j.GatewayServer;
import py4j.Py4JNetworkException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Utility class to provide methods for PySpark integration.
 */
@SuppressWarnings("unused")
public final class SparkPythonUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPythonUtil.class);

  /**
   * Starts a Py4j gateway server.
   *
   * @param dir the local directory for writing information for the gateway server, such as port and auth token.
   * @return the gateway server
   * @throws IOException if failed to start the server or failed to write out the port.
   */
  public static GatewayServer startPy4jGateway(Path dir) throws IOException {
    GatewayServer server = new GatewayServer(null, 0);
    try {
      server.start();
    } catch (Py4JNetworkException e) {
      throw new IOException(e);
    }

    // Write the port number in string form to the port file
    Files.write(dir.resolve(SparkRuntimeUtils.PYSPARK_PORT_FILE_NAME),
                Integer.toString(server.getListeningPort()).getBytes(StandardCharsets.UTF_8));

    LOG.debug("Py4j Gateway server started at port {}", server.getListeningPort());
    return server;
  }

  /**
   * Updates the python callback port in the {@link GatewayServer}.
   */
  public static void setGatewayCallbackPort(GatewayServer gatewayServer, int port) {
    CallbackClient callbackClient = gatewayServer.getCallbackClient();
    InetAddress address = callbackClient.getAddress();
    callbackClient.shutdown();

    Class<? extends GatewayServer> gatewayServerClass = gatewayServer.getClass();
    try {
      callbackClient = new CallbackClient(port, address);

      Field cbClientField = gatewayServerClass.getDeclaredField("cbClient");
      cbClientField.setAccessible(true);
      cbClientField.set(gatewayServer, callbackClient);

      Field gatewayField = gatewayServerClass.getDeclaredField("gateway");
      gatewayField.setAccessible(true);
      Object gateway = gatewayField.get(gatewayServer);
      Field gatewayCbClientField = gateway.getClass().getDeclaredField("cbClient");
      gatewayCbClientField.setAccessible(true);
      gatewayCbClientField.set(gateway, callbackClient);

      Field pythonPortField = gatewayServerClass.getDeclaredField("pythonPort");
      pythonPortField.setAccessible(true);
      pythonPortField.set(gatewayServer, port);
    } catch (Exception e) {
      LOG.warn("Failed to update python gateway callback port. Callback into Python may not work.", e);
    }
  }
}
