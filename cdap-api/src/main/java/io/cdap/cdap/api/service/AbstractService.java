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

package io.cdap.cdap.api.service;

import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.service.http.HttpServiceHandler;
import io.cdap.cdap.internal.api.AbstractPluginConfigurable;
import java.util.Arrays;
import java.util.Map;

/**
 * An abstract implementation of {@link Service}. Users may extend this to write a {@link Service}.
 *
 * The default no-op constructor must be implemented.
 *
 * @param <T> type of service configurer
 * @param <V> type of service context
 */
public abstract class AbstractService<T extends ServiceConfigurer, V extends ServiceContext>
    extends AbstractPluginConfigurable<T> implements Service<T, V> {

  private T configurer;
  private V context;

  @Override
  public final void configure(T serviceConfigurer) {
    this.configurer = serviceConfigurer;
    configure();
  }

  /**
   * Set the name for the Service.
   *
   * @param name of the service.
   */
  protected void setName(String name) {
    configurer.setName(name);
  }

  /**
   * Set the description of the Service.
   *
   * @param description of the service.
   */
  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * Add handler to the Service.
   *
   * @param handler to serve requests with.
   */
  protected void addHandler(HttpServiceHandler handler) {
    addHandlers(Arrays.asList(handler));
  }

  /**
   * Add a list of handlers to the Service.
   *
   * @param handlers to service requests with.
   */
  protected void addHandlers(Iterable<? extends HttpServiceHandler> handlers) {
    configurer.addHandlers(handlers);
  }

  /**
   * Sets the number of instances needed for the server that runs all {@link HttpServiceHandler}s of
   * this Service.
   *
   * @param instances Number of instances, must be > 0.
   */
  protected void setInstances(int instances) {
    configurer.setInstances(instances);
  }

  /**
   * Sets the resources requirements for the server that runs all {@link HttpServiceHandler}s of
   * this Service.
   *
   * @param resources The requirements.
   */
  protected void setResources(Resources resources) {
    configurer.setResources(resources);
  }

  /**
   * Sets a set of properties that will be available through the {@link
   * ServiceSpecification#getProperties()} at runtime.
   *
   * @param properties the properties to set
   */
  protected void setProperties(Map<String, String> properties) {
    configurer.setProperties(properties);
  }

  /**
   * Returns the {@link ServiceConfigurer}, only available at configuration time.
   */
  @Override
  protected final T getConfigurer() {
    return configurer;
  }

  /**
   * Implements this method to configure this Service.
   */
  protected abstract void configure();

  protected V getContext() {
    return context;
  }

  @Override
  @TransactionPolicy(TransactionControl.EXPLICIT)
  public void initialize(V context) throws Exception {
    this.context = context;
  }
}
