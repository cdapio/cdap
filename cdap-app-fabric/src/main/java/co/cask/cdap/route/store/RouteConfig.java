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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Route Configuration.
 */
public class RouteConfig {
  private final Map<String, Integer> routes;
  private transient Boolean isValid;

  public RouteConfig(Map<String, Integer> routes) {
    Preconditions.checkNotNull(routes);
    this.routes = ImmutableMap.copyOf(routes);
  }

  public Map<String, Integer> getRoutes() {
    return routes;
  }

  public boolean isValid() {
    if (isValid == null) {
      int percentageSum = 0;
      for (Integer percent : routes.values()) {
        percentageSum += percent;
      }
      isValid = (percentageSum == 100);
    }
    return isValid;
  }
}
