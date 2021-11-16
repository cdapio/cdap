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

package io.cdap.cdap.logging.gateway.handlers;

import com.google.common.io.Closeables;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Gateway;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpContentConsumer;
import io.cdap.common.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Fetch Program logs via internal REST API calls
 */
public class RemoteLogsFetcher implements LogsFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteLogsFetcher.class);

  private final RemoteClient remoteClient;

  @Inject
  public RemoteLogsFetcher(RemoteClientFactory remoteClientFactory) {
    this.remoteClient =
      remoteClientFactory.createRemoteClient(Constants.Service.LOG_QUERY, new DefaultHttpRequestConfig(false),
                                             Gateway.API_VERSION_3);
  }

  /**
   * Gets the run logs of a program.
   *
   * @param program the program
   * @param runId pipeline run id
   * @param start start time of the time range of desired logs
   * @param stop end time of the time range of desired logs
   * @param file file for us to write the log into
   * @throws IOException              if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway
   *                                  server
   */
  @Override
  public void writeProgramRunLogs(ProgramId program, String runId, long start, long stop, File file)
    throws IOException, UnauthenticatedException, UnauthorizedException {

    String path = String.format("namespaces/%s/apps/%s/%s/%s/runs/%s/logs?start=%d&stop=%d",
                                program.getNamespaceId().getNamespace(), program.getApplication(),
                                program.getType().getCategoryName(), program.getProgram(), runId, start, stop);
    execute(path, file);
  }

  /**
   * Gets the logs of a program.
   *
   * @param componentId component id
   * @param serviceId service id
   * @param start start time of the time range of desired logs
   * @param stop end time of the time range of desired logs
   * @param file file for us to write the log into
   * @throws IOException              if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway
   *                                  server
   */
  @Override
  public void writeSystemServiceLog(String componentId, String serviceId, long start, long stop, File file)
    throws IOException, UnauthenticatedException, UnauthorizedException {
    String path = String.format("system/%s/%s/logs?start=%d&stop=%d", componentId, serviceId, start, stop);
    execute(path, file);
  }

  private void execute(String path, File file) throws IOException, UnauthorizedException {
    try (FileChannel channel = new FileOutputStream(file, false).getChannel()) {
      URL url = remoteClient.resolve(path);
      HttpRequest request = HttpRequest.get(url).withContentConsumer(new HttpContentConsumer() {
        @Override
        public boolean onReceived(ByteBuffer buffer) {
          try {
            channel.write(buffer);
          } catch (IOException e) {
            LOG.error("Failed write to file {}", file);
            return false;
          }
          return true;
        }

        @Override
        public void onFinished() {
          Closeables.closeQuietly(channel);
        }
      }).build();
      remoteClient.executeStreamingRequest(request);
    }
  }
}
