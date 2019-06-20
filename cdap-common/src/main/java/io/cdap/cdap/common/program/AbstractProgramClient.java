/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.common.program;

import com.google.common.reflect.TypeToken;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.common.client.AbstractClient;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Common implementation of methods to interact with app fabric service over HTTP.
 */
public abstract class AbstractProgramClient extends AbstractClient {

  public AbstractProgramClient(DiscoveryServiceClient discoveryClient) {
    super(discoveryClient);
  }

  /**
   * Gets the run records of a program.
   *
   * @param program ID of the program
   * @return the run records of the program
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or program could not be found
   */
  public List<RunRecord> getAllProgramRuns(ProgramId program, long startTime, long endTime, int limit)
    throws IOException, NotFoundException, UnauthorizedException, BadRequestException {
    return getProgramRuns(program, ProgramRunStatus.ALL.name(), startTime, endTime, limit);
  }

  /**
   * Gets the run records of a program.
   *
   * @param program the program
   * @param state - filter by status of the program
   * @return the run records of the program
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or program could not be found
   */
  public List<RunRecord> getProgramRuns(ProgramId program, String state,
                                        long startTime, long endTime, int limit)
    throws IOException, NotFoundException, UnauthorizedException, BadRequestException {

    String queryParams = String.format("%s=%s&%s=%d&%s=%d&%s=%d",
                                       Constants.AppFabric.QUERY_PARAM_STATUS, state,
                                       Constants.AppFabric.QUERY_PARAM_START_TIME, startTime,
                                       Constants.AppFabric.QUERY_PARAM_END_TIME, endTime,
                                       Constants.AppFabric.QUERY_PARAM_LIMIT, limit);
    String path = String.format("namespaces/%s/apps/%s/versions/%s/%s/%s/runs?%s",
                                program.getNamespace(), program.getApplication(), program.getVersion(),
                                program.getType().getCategoryName(), program.getProgram(), queryParams);
    HttpResponse response = makeRequest(path, HttpMethod.GET, null);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(program);
    }

    return ObjectResponse.fromJsonBody(response, new TypeToken<List<RunRecord>>() {
    }).getResponseObject();
  }

  private HttpResponse makeRequest(String path, HttpMethod httpMethod, @Nullable String body)
    throws IOException, BadRequestException, UnauthorizedException {
    URL url = resolve(path);
    HttpRequest.Builder builder = HttpRequest.builder(httpMethod, url);
    if (body != null) {
      builder.withBody(body);
    }
    HttpResponse response = execute(builder.build(),
                                    HttpURLConnection.HTTP_BAD_REQUEST, HttpURLConnection.HTTP_NOT_FOUND);
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(response.getResponseBodyAsString());
    }
    return response;
  }

}
