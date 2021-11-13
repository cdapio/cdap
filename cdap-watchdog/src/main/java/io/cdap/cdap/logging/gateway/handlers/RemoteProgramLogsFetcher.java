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

import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProgramNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Gateway;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.stream.Stream;

/**
 * Fetch Program logs via internal REST API calls
 */
public class RemoteProgramLogsFetcher implements ProgramLogsFetcher {
  private final RemoteClient remoteClient;

  @Inject
  public RemoteProgramLogsFetcher(RemoteClientFactory remoteClientFactory) {
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
   * @return the logs of the program
   * @throws IOException              if a network error occurred
   * @throws NotFoundException        if the application or program could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway
   *                                  server
   */
  @Override
  public Stream<String> getProgramRunLogs(ProgramId program, String runId, long start, long stop)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException {

    String url = String.format("namespaces/%s/apps/%s/%s/%s/runs/%s/logs?start=%d&stop=%d",
                               program.getNamespaceId().getNamespace(), program.getApplication(),
                               program.getType().getCategoryName(), program.getProgram(), runId, start, stop);
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    HttpResponse response;
    response = execute(requestBuilder.build());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new ProgramNotFoundException(program);
    }

    return response.getResponseBodyAsString().codePoints().mapToObj(c -> String.valueOf((char) c));
  }

  /**
   * Gets the logs of a program.
   *
   * @param componentId component id
   * @param serviceId service id
   * @param start start time of the time range of desired logs
   * @param stop end time of the time range of desired logs
   * @return the log of the program
   * @throws IOException              if a network error occurred
   * @throws NotFoundException        if the application or program could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway
   *                                  server
   */
  @Override
  public Stream<String> getProgramSystemLog(String componentId, String serviceId, long start, long stop)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException {

    String url = String.format("system/%s/%s/logs?start=%d&stop=%d", componentId, serviceId, start, stop);
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    HttpResponse response;
    response = execute(requestBuilder.build());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(String.format("system log not found with service", serviceId));
    }

    return response.getResponseBodyAsString().codePoints().mapToObj(c -> String.valueOf((char) c));
  }

  private HttpResponse execute(HttpRequest request) throws IOException, NotFoundException, UnauthorizedException {
    HttpResponse httpResponse = remoteClient.execute(request);
    if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(httpResponse.getResponseBodyAsString());
    }
    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException(String.format("Request failed %s", httpResponse.getResponseBodyAsString()));
    }
    return httpResponse;
  }
}
