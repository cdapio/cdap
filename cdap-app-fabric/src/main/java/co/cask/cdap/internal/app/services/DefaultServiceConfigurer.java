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

import co.cask.cdap.api.service.ServiceConfigurer;
import co.cask.cdap.api.service.ServiceWorker;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A default implementation of {@link ServiceConfigurer}.
 */
public class DefaultServiceConfigurer implements ServiceConfigurer {
  private String description;
  private String name;
  private Map<String, String> properties;
  private List<ServiceWorker> workers;
  private List<HttpServiceHandler> handlers;
  private Set<String> datasets;

  /**
   * Create an instance of {@link DefaultServiceConfigurer}
   */
  public DefaultServiceConfigurer() {
    this.workers = Lists.newArrayList();
    this.properties = Maps.newHashMap();
    this.handlers = Lists.newArrayList();
    this.datasets = Sets.newHashSet();
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
  public <T extends ServiceWorker> void addWorker(T worker) {
    workers.add(worker);
  }

  @Override
  public <T extends ServiceWorker> void addWorkers(Iterable<T> serviceWorkers) {
    Iterables.addAll(workers, serviceWorkers);
  }

  @Override
  public <T extends HttpServiceHandler> void addHandler(T serviceHandler) {
    handlers.add(serviceHandler);
  }

  @Override
  public <T extends HttpServiceHandler> void addHandlers(Iterable<T> serviceHandlers) {
    Iterables.addAll(handlers, serviceHandlers);
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    this.properties = ImmutableMap.copyOf(properties);
  }

  @Override
  public List<HttpServiceHandler> getHandlers() {
    return handlers;
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

  @Override
  public Map<String, String> getProperties() {
    return this.properties;
  }

  @Override
  public void useDataset(String dataset) {
    datasets.add(dataset);
  }

  @Override
  public void useDatasets(Iterable<String> datasets) {
    Iterables.addAll(this.datasets, datasets);
  }

  @Override
  public Set<String> getDatasets() {
    return datasets;
  }
}
