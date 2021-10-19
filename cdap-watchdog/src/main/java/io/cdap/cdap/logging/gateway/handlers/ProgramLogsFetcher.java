/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProgramNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
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
 * Interface for fetching {@link RunRecordDetail}
 */
public interface ProgramLogsFetcher {
  /**
   * Gets the run logs of a program.
   *
   * @param program the program
   * @param runId pipeline run id
   * @param start start time of the time range of desired logs
   * @param stop end time of the time range of desired logs
   * @return the logs of the program
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or program could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  String getProgramRunLogs(ProgramId program, String runId, long start, long stop)
      throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException;

  /**
   * Gets the logs of a program.
   *
   * @param componentId component id
   * @param serviceId service id
   * @param start start time of the time range of desired logs
   * @param stop end time of the time range of desired logs
   * @return the log of the program
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or program could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  String getProgramSystemLog(String componentId, String serviceId, long start, long stop)
      throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException;
}
