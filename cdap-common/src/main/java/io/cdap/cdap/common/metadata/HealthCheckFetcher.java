/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common.metadata;

import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for fetching {@code AppFabricHealthCheck}
 */
public interface HealthCheckFetcher {

  /**
   * Get the app fabric health check
   *
   * @param serviceName the name of the current service
   * @param namespace the current namespace
   * @param instanceName the current instance name
   * @param podLabelSelector the label to identify the pod
   * @param nodeFieldSelector the label to identify the node
   * @return the health check data response
   * @throws IOException       if failed to get {@code ApplicationDetail}
   * @throws NotFoundException if the application or namespace identified by the supplied id doesn't exist
   */
  Map<String, Object> getHealthDetails(String serviceName, String namespace, String instanceName,
                                       String podLabelSelector, String nodeFieldSelector)
    throws IOException, NotFoundException, UnauthorizedException;
}
