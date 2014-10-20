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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.api.service.ServiceConfigurer;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.ServiceWorker;
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceSpecification;
import co.cask.cdap.internal.service.DefaultServiceSpecification;
import com.clearspring.analytics.util.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * A default implementation of {@link ServiceConfigurer}.
 */
public class DefaultServiceConfigurer implements ServiceConfigurer {
  private final String className;
  private String name;
  private String description;
  private Map<String, ServiceWorkerSpecification> workers;
  private Map<String, HttpServiceSpecification> handlers;
  private Resources resources;
  private int instances;

  /**
   * Create an instance of {@link DefaultServiceConfigurer}
   */
  public DefaultServiceConfigurer(Service service) {
    this.className = service.getClass().getName();
    this.name = service.getClass().getSimpleName();
    this.description = "";
    this.workers = Maps.newHashMap();
    this.handlers = Maps.newHashMap();
    this.resources = new Resources();
    this.instances = 1;
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
  public void addWorkers(Map<String, ServiceWorker> serviceWorkers) {
    for (Map.Entry<String, ServiceWorker> entry : serviceWorkers.entrySet()) {
      String name = entry.getKey();
      ServiceWorker worker = entry.getValue();

      Preconditions.checkArgument(!name.equals(this.name),
                                  "Service worker cannot has the same name as the enclosing Service.");
      Preconditions.checkArgument(!workers.containsKey(name),
                                  "Service worker with name %s already existed.", name);

      DefaultServiceWorkerConfigurer configurer = new DefaultServiceWorkerConfigurer(name, worker);
      worker.configure(configurer);
      workers.put(name, configurer.createSpecification());
    }
  }

  @Override
  public void addHandlers(Iterable<? extends HttpServiceHandler> serviceHandlers) {
    for (HttpServiceHandler handler : serviceHandlers) {
      DefaultHttpServiceHandlerConfigurer configurer = new DefaultHttpServiceHandlerConfigurer(handler);
      handler.configure(configurer);
      HttpServiceSpecification spec = configurer.createSpecification();
      Preconditions.checkArgument(!handlers.containsKey(spec.getName()),
                                  "Handler with name %s already existed.", spec.getName());
      handlers.put(spec.getName(), spec);
    }
  }

  @Override
  public void setInstances(int instances) {
    Preconditions.checkArgument(instances > 0, "Instances must be > 0.");
    this.instances = instances;
  }

  @Override
  public void setResources(Resources resources) {
    Preconditions.checkArgument(resources != null, "Resources cannot be null.");
    this.resources = resources;
  }

  public ServiceSpecification createSpecification() {
    Preconditions.checkArgument(!handlers.isEmpty(), "Cannot define a Service without handler.");
    return new DefaultServiceSpecification(className, name, description, handlers, workers, resources, instances);
  }
}
