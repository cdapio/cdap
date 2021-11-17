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

import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.File;
import java.io.IOException;

/**
 * Interface for fetching Program logs
 */
public interface LogsFetcher {
  /**
   * Gets the run logs of a program.
   *
   * @param program the program
   * @param runId pipeline run id
   * @param start start time of the time range of desired logs
   * @param stop end time of the time range of desired logs
   * @param file file for us to write the log into
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  void writeProgramRunLogs(ProgramId program, String runId, long start, long stop, File file)
    throws IOException, UnauthenticatedException, UnauthorizedException;

  /**
   * Gets the logs of a program.
   *
   * @param componentId component id
   * @param serviceId service id
   * @param start start time of the time range of desired logs
   * @param stop end time of the time range of desired logs
   * @param file path for the file to write into
   * @throws IOException if a network error occurred
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  void writeSystemServiceLog(String componentId, String serviceId, long start, long stop, File file)
    throws IOException, UnauthenticatedException, UnauthorizedException;
}
