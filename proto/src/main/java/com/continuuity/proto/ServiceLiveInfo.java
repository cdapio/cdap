/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.proto;

import org.apache.twill.discovery.Discoverable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ServiceLiveInfo has details for a service including a list of discoverables.
 */
public class ServiceLiveInfo {
  private final String name;
  private final List<Map<String, Object>> discoverables;

  /**
   * Create an instance of ServiceLiveInfo with a given set of discoverables.
   * @param name Name of service.
   * @param discoverables Discoverables for the service.
   */
  public ServiceLiveInfo(String name, List<Discoverable> discoverables) {
    this.name = name;
    this.discoverables = new ArrayList<Map<String, Object>>();
    for (Discoverable discoverable : discoverables) {
      addDiscoverable(discoverable);
    }
  }

  /**
   * Create an instance of ServiceLiveInfo.
   * @param name Name of service.
   */
  public ServiceLiveInfo(String name) {
    this(name, new ArrayList<Discoverable>());
  }

  /**
   * Add a discoverable to this ServiceLiveInfo.
   * @param discoverable discoverable to be added.
   */
  public void addDiscoverable(Discoverable discoverable) {
    Map<String, Object> discoverableInfo = new HashMap<String, Object>();
    discoverableInfo.put("host", discoverable.getSocketAddress().getHostName());
    discoverableInfo.put("port", discoverable.getSocketAddress().getPort());
    discoverables.add(discoverableInfo);
  }

  /**
   * Get name of service.
   * @return name of service.
   */
  public String getName() {
    return name;
  }
}
