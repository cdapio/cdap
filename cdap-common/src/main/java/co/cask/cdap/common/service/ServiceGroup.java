/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Service that maintains a set of {@link ManagedService}s,
 * arranges them into phases depending on which services
 * they depend on, and starts those services phase by phase.
 */
public class ServiceGroup extends AbstractIdleService {

  private final List<ServiceGroupPhase> servicePhases;

  public ServiceGroup(Iterable<ManagedService> services) {
    this.servicePhases = createPhases(services);
  }

  private List<ServiceGroupPhase> createPhases(Iterable<ManagedService> services) {
    List<ManagedService> remainingServices = Lists.newArrayList();
    for (ManagedService service : services) {
      remainingServices.add(service);
    }

    List<ServiceGroupPhase> phases = Lists.newArrayList();
    Set<ServiceId> availableServices = Sets.newHashSet();

    while (!remainingServices.isEmpty()) {
      ServiceGroupPhase currentPhase = new ServiceGroupPhase();
      Set<ServiceId> currentPhaseAvailableServices = Sets.newHashSet();

      Iterator<ManagedService> iterator = remainingServices.iterator();
      while (iterator.hasNext()) {
        ManagedService managedService = iterator.next();
        ServiceId serviceId = managedService.getServiceId();

        Set<ServiceId> serviceDependencies = managedService.getServiceDependencies();
        if (allDependenciesFulfilled(availableServices, serviceDependencies)) {
          currentPhaseAvailableServices.add(serviceId);
          currentPhase.add(managedService);
          iterator.remove();
        }
      }

      availableServices.addAll(currentPhaseAvailableServices);
      phases.add(currentPhase);
    }
    return phases;
  }

  private boolean allDependenciesFulfilled(Set<ServiceId> availableServices, Set<ServiceId> serviceDependencies) {
    for (ServiceId serviceDependency : serviceDependencies) {
      if (!availableServices.contains(serviceDependency)) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected void startUp() throws Exception {
    for (ServiceGroupPhase servicePhase : servicePhases) {
      servicePhase.startAndWait();
    }
  }

  @Override
  protected void shutDown() throws Exception {
    for (ServiceGroupPhase servicePhase : Lists.reverse(servicePhases)) {
      servicePhase.stopAndWait();
    }
  }

  public List<ServiceGroupPhase> getPhases() {
    return servicePhases;
  }
}
