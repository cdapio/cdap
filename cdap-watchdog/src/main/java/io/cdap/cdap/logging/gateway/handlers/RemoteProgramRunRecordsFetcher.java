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

import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Gateway;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;

/**
 * Fetch List of {@link RunRecord} via internal REST API calls
 */
public class RemoteProgramRunRecordsFetcher implements ProgramRunRecordsFetcher {

  private final RemoteClient remoteClient;

  @Inject
  public RemoteProgramRunRecordsFetcher(RemoteClientFactory remoteClientFactory) {
    this.remoteClient =
      remoteClientFactory.createRemoteClient(Constants.Service.APP_FABRIC_HTTP, new DefaultHttpRequestConfig(false),
                                             Gateway.API_VERSION_3);
  }

  /**
   * Gets the run records of a program.
   *
   * @param program the program
   * @return the run records of the program
   * @throws IOException              if a network error occurred
   * @throws NotFoundException        if the application or program could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  @Override
  public Iterable<RunRecord> getProgramRuns(ProgramId program, long startTime, long endTime, int limit)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException {

    String queryParams =
      String.format("%s=%s&%s=%d&%s=%d&%s=%d", Constants.AppFabric.QUERY_PARAM_STATUS, ProgramRunStatus.ALL.name(),
                    Constants.AppFabric.QUERY_PARAM_START_TIME, startTime, Constants.AppFabric.QUERY_PARAM_END_TIME,
                    endTime, Constants.AppFabric.QUERY_PARAM_LIMIT, limit);

    String url =
      String.format("namespaces/%s/apps/%s/versions/%s/%s/%s/runs?%s", program.getNamespaceId().getNamespace(),
                    program.getApplication(), program.getVersion(), program.getType().getCategoryName(),
                    program.getProgram(), queryParams);
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    HttpResponse response;
    response = execute(requestBuilder.build());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(program);
    }

    return ObjectResponse.fromJsonBody(response, new TypeToken<List<RunRecord>>() { }).getResponseObject();
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
