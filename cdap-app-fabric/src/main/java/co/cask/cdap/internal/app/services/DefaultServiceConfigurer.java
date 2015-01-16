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
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.internal.app.runtime.service.http.DelegatorContext;
import co.cask.cdap.internal.app.runtime.service.http.HttpHandlerFactory;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.clearspring.analytics.util.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A default implementation of {@link ServiceConfigurer}.
 */
public class DefaultServiceConfigurer implements ServiceConfigurer {
  private final String className;
  private String name;
  private String description;
  private Map<String, ServiceWorkerSpecification> workers;
  private List<HttpServiceHandler> handlers;
  private Resources resources;
  private int instances;
  private static final Logger LOG = LoggerFactory.getLogger(DefaultServiceConfigurer.class);

  /**
   * Create an instance of {@link DefaultServiceConfigurer}
   */
  public DefaultServiceConfigurer(Service service) {
    this.className = service.getClass().getName();
    this.name = service.getClass().getSimpleName();
    this.description = "";
    this.workers = Maps.newHashMap();
    this.handlers = Lists.newArrayList();
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
    Iterables.addAll(handlers, serviceHandlers);
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
    Map<String, HttpServiceHandlerSpecification> handleSpecs = createHandlerSpecs(handlers);
    return new ServiceSpecification(className, name, description, handleSpecs, workers, resources, instances);
  }

  /**
   * Constructs HttpServiceSpecifications for each of the handlers in the {@param handlers} list.
   * Also performs verifications on these handlers (that a NettyHttpService can be constructed from them).
   */
  private Map<String, HttpServiceHandlerSpecification> createHandlerSpecs(List<? extends HttpServiceHandler> handlers) {
    verifyHandlers(handlers);
    Map<String, HttpServiceHandlerSpecification> handleSpecs = Maps.newHashMap();
    for (HttpServiceHandler handler : handlers) {
      DefaultHttpServiceHandlerConfigurer configurer = new DefaultHttpServiceHandlerConfigurer(handler);
      handler.configure(configurer);
      HttpServiceHandlerSpecification spec = configurer.createSpecification();
      Preconditions.checkArgument(!handleSpecs.containsKey(spec.getName()),
                                  "Handler with name %s already existed.", spec.getName());
      handleSpecs.put(spec.getName(), spec);
    }
    return handleSpecs;
  }

  private void verifyHandlers(List<? extends HttpServiceHandler> handlers) {
    Preconditions.checkArgument(!Iterables.isEmpty(handlers), "Service %s should have at least one handler", name);
    try {
      List<HttpHandler> httpHandlers = Lists.newArrayList();
      for (HttpServiceHandler handler : handlers) {
        httpHandlers.add(createHttpHandler(handler));
      }

      // Constructs a NettyHttpService, to verify that the handlers passed in by the user are valid.
      NettyHttpService.builder()
        .addHttpHandlers(httpHandlers)
        .build();
    } catch (Throwable t) {
      String errMessage = String.format("Invalid handlers in service: %s.", name);
      LOG.error(errMessage, t);
      throw new IllegalArgumentException(errMessage, t);
    }

  }

  private <T extends HttpServiceHandler> HttpHandler createHttpHandler(T handler) {
    MetricsCollector noOpsMetricsCollector =
      new NoOpMetricsCollectionService().getCollector(MetricsScope.SYSTEM, new HashMap<String, String>());
    HttpHandlerFactory factory = new HttpHandlerFactory("", noOpsMetricsCollector);
    @SuppressWarnings("unchecked")
    TypeToken<T> type = (TypeToken<T>) TypeToken.of(handler.getClass());
    return factory.createHttpHandler(type, new VerificationDelegateContext<T>(handler));
  }

  private static final class VerificationDelegateContext<T extends HttpServiceHandler> implements DelegatorContext<T> {

    private final T handler;

    private VerificationDelegateContext(T handler) {
      this.handler = handler;
    }

    @Override
    public T getHandler() {
      return handler;
    }

    @Override
    public HttpServiceContext getServiceContext() {
      // Never used. (It's only used during server runtime, which we don't verify).
      return null;
    }
  }
}
