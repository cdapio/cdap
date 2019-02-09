/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.gateway.discovery;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.route.store.RouteConfig;
import co.cask.cdap.route.store.RouteStore;
import com.google.common.collect.Maps;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * An in memory implementation of {@link RouteStore} for unit test.
 */
public class InMemoryRouteStore implements RouteStore {
  private final Map<ProgramId, RouteConfig> routeMap;

  public InMemoryRouteStore(Map<ProgramId, RouteConfig> routeMap) {
    this.routeMap = Maps.newHashMap(routeMap);
  }

  @Override
  public void store(ProgramId serviceId, RouteConfig routeConfig) {
    routeMap.put(serviceId, routeConfig);
  }

  @Override
  public void delete(ProgramId serviceId) throws NotFoundException {
    routeMap.remove(serviceId);
  }

  @Nullable
  @Override
  public RouteConfig fetch(ProgramId serviceId) {
    return routeMap.get(serviceId);
  }

  @Override
  public void close() throws Exception {
    // nothing to do
  }
}
