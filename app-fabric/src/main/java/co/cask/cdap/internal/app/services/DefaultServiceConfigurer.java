/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.service.ServiceConfigurer;
import co.cask.cdap.api.service.ServiceWorker;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * A default implementation of Configurer for Services.
 */
public class DefaultServiceConfigurer implements ServiceConfigurer {
  private String description;
  private String name;
  private List<ServiceWorker> workers;
  private HttpServiceHandler serviceHandler;

  /**
   * Create an instance of {@link ServiceConfigurer}
   */
  public DefaultServiceConfigurer() {
    this.workers = Lists.newArrayList();
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void addWorker(ServiceWorker worker) {
    workers.add(worker);
  }

  @Override
  public void setHandler(HttpServiceHandler serviceHandler) {
    this.serviceHandler = serviceHandler;
  }

  @Override
  public HttpServiceHandler getHandler() {
    return serviceHandler;
  }

  @Override
  public List<ServiceWorker> getWorkers() {
    return workers;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }
}
