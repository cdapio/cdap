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

package co.cask.cdap.api.service;

import co.cask.cdap.api.annotation.Beta;
import org.apache.twill.api.ResourceSpecification;

/**
 * Extend this class to add workers to a custom Service.
 */
@Beta
public abstract class AbstractServiceWorker implements ServiceWorker {
  private ServiceWorkerContext context;
  private ServiceWorkerConfigurer configurer;

  /**
   * User can optionally override this method to configure the ServiceWorker.
   */
  public void configure() {

  }

  @Override
  public final void configure(ServiceWorkerConfigurer configurer) {
    this.configurer = configurer;

    configure();
  }

  /**
   * Sets the ServiceWorker's name.
   */
  public void setName(String name) {
    configurer.setName(name);
  }

  /**
   * Sets the ServiceWorker's description.
   */
  public void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * Sets the ServiceWorker's ResourceSpecification.
   */
  public void setResourceSpecification(ResourceSpecification resourceSpecification) {
    configurer.setResourceSpecification(resourceSpecification);
  }

  @Override
  public void initialize(ServiceWorkerContext context) throws Exception {
    this.context = context;
  }

  protected ServiceWorkerContext getContext() {
    return context;
  }

  @Override
  public void stop() {
    // default no-op
  }

  @Override
  public void destroy() {
    // default no-op
  }
}
