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

import co.cask.cdap.internal.service.DefaultServiceSpecification;

/**
 * An abstract implementation of {@link Service}. Users may extend this to write a {@link Service}.
 *
 * The default no-op constructor must be implemented.
 */
public abstract class AbstractService implements Service {
  protected ServiceConfigurer configurer;

  @Override
  public final ServiceSpecification configure(ServiceConfigurer serviceConfigurer) {
    this.configurer = serviceConfigurer;
    configure();

    return new DefaultServiceSpecification(getClass().getSimpleName(), configurer.getName(),
                                           configurer.getDescription(), configurer.getProperties(),
                                           configurer.getWorkers(), configurer.getHandlers());
  }

  /**
   * Implement this method and use a {@link ServiceConfigurer} to add a request handler
   * and workers.
   */
  protected abstract void configure();

}
