/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.route.store;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.id.ProgramId;

/**
 * Provides the ability to store and retrieve user service routing configuration.
 */
public interface RouteStore extends AutoCloseable {

  /**
   * Store a {@link RouteConfig} for a given {@link ProgramId}. If a {@link RouteConfig} already exists,
   * it is overwritten.
   *
   * @param serviceId Id of the User Service
   * @param routeConfig {@link RouteConfig}
   */
  void store(ProgramId serviceId, RouteConfig routeConfig);

  /**
   * Delete any {@link RouteConfig} for a given {@link ProgramId}. If a {@link RouteConfig} doesn't exist,
   * {@link NotFoundException} is thrown.
   *
   * @param serviceId Id of the User Service
   */
  void delete(ProgramId serviceId) throws NotFoundException;


  /**
   * Get the {@link RouteConfig} for a given {@link ProgramId}.
   *
   * @param serviceId Id of the User Service
   * @return {@link RouteConfig}
   */
  RouteConfig fetch(ProgramId serviceId);
}
