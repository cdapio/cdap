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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.service.http.HttpServiceHandler;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * An abstract implementation of {@link Service}. Users may extend this to write a {@link Service}.
 *
 * The default no-op constructor must be implemented.
 */
public abstract class AbstractService implements Service {
  private ServiceConfigurer configurer;

  @Override
  public final void configure(ServiceConfigurer serviceConfigurer) {
    this.configurer = serviceConfigurer;
    configure();
  }

  /**
   * Set the name for the Service.
   * @param name of the service.
   */
  protected void setName(String name) {
    configurer.setName(name);
  }

  /**
   * Set the description of the Service.
   * @param description of the service.
   */
  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * Add handler to the Service.
   * @param handler to serve requests with.
   */
  protected void addHandler(HttpServiceHandler handler) {
    addHandlers(Arrays.asList(handler));
  }

  /**
   * Add a list of handlers to the Service.
   * @param handlers to service requests with.
   */
  protected void addHandlers(Iterable<? extends HttpServiceHandler> handlers) {
    configurer.addHandlers(handlers);
  }

  /**
   * Add a worker to the Service.
   * @deprecated as of version 2.8.0, replaced by {@link AbstractApplication#addWorker}
   * @param worker for the service.
   */
  @Deprecated
  protected void addWorker(String name, ServiceWorker worker) {
    Map<String, ServiceWorker> workers = new HashMap<String, ServiceWorker>();
    workers.put(name, worker);
    addWorkers(workers);
  }

  /**
   * Adds workers to the Service. The worker simple class name is used as the worker name.
   * @deprecated as of version 2.8.0, replaced by {@link AbstractApplication#addWorker}
   * @param worker The worker to add
   * @param workers More workers to add.
   */
  @Deprecated
   protected void addWorkers(ServiceWorker worker, ServiceWorker...workers) {
    Map<String, ServiceWorker> workerMap = new HashMap<String, ServiceWorker>();
    workerMap.put(worker.getClass().getSimpleName(), worker);
    for (ServiceWorker serviceWorker : workers) {
      workerMap.put(serviceWorker.getClass().getName(), serviceWorker);
    }
    addWorkers(workerMap);
   }

  /**
   * Add a list of workers to the Service.
   * @deprecated as of version 2.8.0, replaced by {@link AbstractApplication#addWorker}
   * @param workers for the service.
   */
  @Deprecated
  protected void addWorkers(Map<String, ServiceWorker> workers) {
    configurer.addWorkers(workers);
  }

  /**
   * Sets the number of instances needed for the server that runs all {@link HttpServiceHandler}s of this Service.
   * @param instances Number of instances, must be > 0.
   */
  protected void setInstances(int instances) {
    configurer.setInstances(instances);
  }

  /**
   * Sets the resources requirements for the server that runs all {@link HttpServiceHandler}s of this Service.
   * @param resources The requirements.
   */
  protected void setResources(Resources resources) {
    configurer.setResources(resources);
  }

  /**
   * Returns the {@link ServiceConfigurer}, only available at configuration time.
   */
  protected final ServiceConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * Implements this method to configure this Service.
   */
  protected abstract void configure();
}
