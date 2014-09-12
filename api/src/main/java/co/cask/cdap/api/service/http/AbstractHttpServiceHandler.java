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

package co.cask.cdap.api.service.http;

import java.util.Map;

/**
 * An abstract implementation of {@link HttpServiceHandler}. Classes that extend this class only
 * have to implement a configure method which can be used to add optional arguments.
 */
public abstract class AbstractHttpServiceHandler implements HttpServiceHandler {
  private HttpServiceConfigurer configurer;
  private HttpServiceContext context;

  /**
   * This can be overridden in child classes to add custom user properties during configure time.
   */
  protected void configure() {
    // no-op
  }

  /**
   * An implementation of {@link HttpServiceHandler#configure(HttpServiceConfigurer)}. Stores the configurer
   * so that it can be used later and then runs the configure method which is overwritten by children classes.
   *
   * @param configurer the {@link HttpServiceConfigurer} which is used to configure this Handler
   */
  @Override
  public final void configure(HttpServiceConfigurer configurer) {
    this.configurer = configurer;
    configure();
  }

  /**
   * An implementation of {@link HttpServiceHandler#initialize(HttpServiceContext)}. Stores the context
   * so that it can be used later.
   *
   * @param context the HTTP service runtime context
   * @throws Exception
   */
  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    this.context = context;
  }

  /**
   * An implementation of {@link HttpServiceHandler#destroy()} which does nothing
   */
  @Override
  public void destroy() {
    // no-op
  }

  /**
   * @return the {@link HttpServiceContext} which was used when this class was initialized
   */
  protected final HttpServiceContext getContext() {
    return context;
  }

  /**
   * @return the {@link HttpServiceConfigurer} used to configure this class
   */
  protected final HttpServiceConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * @see HttpServiceConfigurer#setName(String)
   *
   * @param name the name to set
   */
  protected final void setName(String name) {
    configurer.setName(name);
  }

  /**
   * @see HttpServiceConfigurer#setDescription(String)
   *
   * @param description the description to set
   */
  protected final void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * @see HttpServiceConfigurer#setArguments(java.util.Map)
   *
   * @param arguments the runtime arguments to store
   */
  protected final void setArguments(Map<String, String> arguments) {
    configurer.setArguments(arguments);
  }
}
