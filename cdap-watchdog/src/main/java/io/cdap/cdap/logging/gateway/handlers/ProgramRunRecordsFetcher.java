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

import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;

/**
 * Interface for fetching List of {@link RunRecord}
 */
public interface ProgramRunRecordsFetcher {
  /**
   * Gets the run records of a program.
   *
   * @param program the program
   * @return the run records of the program
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the application or program could not be found
   * @throws UnauthenticatedException if the request is not authorized successfully in the gateway server
   */
  Iterable<RunRecord> getProgramRuns(ProgramId program, long startTime, long endTime, int limit)
    throws IOException, NotFoundException, UnauthenticatedException, UnauthorizedException;
}
