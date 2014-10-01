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
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * A phase in the lifecycle of a {@link ServiceGroup}.
 */
public class ServiceGroupPhase extends AbstractIdleService {

  private final List<Service> services;

  public ServiceGroupPhase(List<Service> services) {
    this.services = services;
  }

  public ServiceGroupPhase() {
    this(new ArrayList<Service>());
  }

  @Override
  protected void startUp() throws Exception {
    List<ListenableFuture<State>> futures = Lists.newArrayList();
    for (Service service : services) {
      futures.add(service.start());
    }

    for (ListenableFuture<State> future : futures) {
      Futures.getUnchecked(future);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    List<ListenableFuture<State>> futures = Lists.newArrayList();
    for (Service service : services) {
      futures.add(service.stop());
    }

    for (ListenableFuture<State> future : futures) {
      Futures.getUnchecked(future);
    }
  }

  public void add(Service service) {
    services.add(service);
  }

  public List<Service> getServices() {
    return services;
  }
}
