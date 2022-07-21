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
import py4j.GatewayServer;
import py4j.Py4JNetworkException;

import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.EnumSet;

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
    // 256 bits secret
    byte[] secret = new byte[256 / 8];
    new SecureRandom().nextBytes(secret);
    String authToken = Base64.getEncoder().encodeToString(secret);

    GatewayServer server = new GatewayServer.GatewayServerBuilder()
      .javaPort(0)
      .authToken(authToken)
      .build();
    try {
      server.start();
    } catch (Py4JNetworkException e) {
      throw new IOException(e);
    }

    // Write the port number in string form to the port file
    Files.write(dir.resolve(SparkRuntimeUtils.PYSPARK_PORT_FILE_NAME),
                Integer.toString(server.getListeningPort()).getBytes(StandardCharsets.UTF_8));
    // Write the auth token
    Path secretFile = dir.resolve(SparkRuntimeUtils.PYSPARK_SECRET_FILE_NAME);
    try (ByteChannel channel = Files.newByteChannel(dir.resolve(SparkRuntimeUtils.PYSPARK_SECRET_FILE_NAME),
                                               EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE),
                                               PosixFilePermissions.asFileAttribute(
                                                 EnumSet.of(PosixFilePermission.OWNER_READ,
                                                            PosixFilePermission.OWNER_WRITE)))) {
      channel.write(StandardCharsets.UTF_8.encode(authToken));
    }

    LOG.debug("Py4j Gateway server started at port {} with auth token of {} bits",
              server.getListeningPort(), secret.length);

    return server;
  }

  /**
   * Updates the python callback port in the {@link GatewayServer}.
   */
  public static void setGatewayCallbackPort(GatewayServer gatewayServer, int port) {
    gatewayServer.resetCallbackClient(gatewayServer.getCallbackClient().getAddress(), port);
  }

  private SparkPythonUtil() {
    // no-op
  }
}
