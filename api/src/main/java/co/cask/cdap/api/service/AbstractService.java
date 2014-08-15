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

package co.cask.cdap.api.service;

import co.cask.cdap.api.service.http.HttpServiceHandler;

import java.util.List;

/**
 * An abstract implementation of {@link co.cask.cdap.api.service.Service}. Users may extend this to write their own
 * custom service.
 */
public abstract class AbstractService implements Service {
  protected ServiceConfigurer configurer;

  @Override
  public String getName() {
    return this.configurer.getName();
  }

  @Override
  public final void configure(ServiceConfigurer serviceConfigurer) {
    this.configurer = serviceConfigurer;
    configure();
  }

  @Override
  public HttpServiceHandler getHandler() {
    return configurer.getHandler();
  }

  @Override
  public List<ServiceWorker> getWorkers() {
    return configurer.getWorkers();
  }

  /**
   * Implement this method and use a {@link co.cask.cdap.api.service.ServiceConfigurer} to add a request handler
   * and workers.
   */
  protected abstract void configure();

}
