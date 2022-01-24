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

package io.cdap.cdap.metadata;

import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Interface for fetching {@code AppFabricHealthCheck}
 */
public interface AppFabricHealthCheckFetcher {

  /**
   * Get the app fabric health check
   * @return the health check data response
   * @throws IOException if failed to get {@code ApplicationDetail}
   * @throws NotFoundException if the application or namespace identified by the supplied id doesn't exist
   */
  Optional<Map<String, Object>> getHealthDetails() throws IOException, NotFoundException, UnauthorizedException;
}
