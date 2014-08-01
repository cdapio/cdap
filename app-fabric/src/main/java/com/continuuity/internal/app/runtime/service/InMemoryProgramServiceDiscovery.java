/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.app.runtime.service;

import com.continuuity.internal.app.runtime.ProgramServiceDiscovery;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;

/**
 * InMemory ProgramServiceDiscovery which implements the discovery of User Service.
 */
public class InMemoryProgramServiceDiscovery implements ProgramServiceDiscovery {

  private DiscoveryServiceClient dsClient;

  @Inject
  public InMemoryProgramServiceDiscovery(DiscoveryServiceClient discoveryService) {
    this.dsClient = discoveryService;
  }

  @Override
  public ServiceDiscovered discover(String accountId, String appId, String serviceId, String serviceName) {
    return dsClient.discover(String.format("service.%s.%s.%s.%s", accountId, appId, serviceId, serviceName));
  }
}
