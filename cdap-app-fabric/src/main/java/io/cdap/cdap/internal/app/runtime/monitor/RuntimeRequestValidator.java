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

package io.cdap.cdap.internal.app.runtime.monitor;

import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.GoneException;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.netty.handler.codec.http.HttpRequest;
import java.util.Optional;

/**
 * Defines validation of incoming requests from program runtime.
 */
public interface RuntimeRequestValidator {

  /**
   * Validates an incoming request from a program runtime and returns the {@link
   * ValidatedRuntimeRequest} containing the {@link ProgramRunInfo} (and authenticated run identity
   * and scopes when a {@code RUNTIME} token was presented).
   *
   * @param programRunId the program run id from the request URL
   * @param request the http request from the program runtime
   * @throws BadRequestException if the request is not valid
   * @throws GoneException if the run already finished
   * @throws AccessException if the request fails authentication or authorization
   */
  ValidatedRuntimeRequest validate(ProgramRunId programRunId, HttpRequest request)
      throws BadRequestException, AccessException, GoneException;

  /**
   * Returns the top-level program run for {@code programRunId} (either {@code programRunId} itself
   * or its parent workflow run), or {@link Optional#empty()} when no record exists locally or
   * remotely.
   *
   * @param programRunId the program run id to resolve
   * @return the top-level program run id, or empty if no record exists
   * @throws ServiceUnavailableException if the run record cannot be looked up due to a transient
   *     local or remote error
   */
  default Optional<ProgramRunId> getTopLevelRun(ProgramRunId programRunId) {
    return Optional.empty();
  }
}
