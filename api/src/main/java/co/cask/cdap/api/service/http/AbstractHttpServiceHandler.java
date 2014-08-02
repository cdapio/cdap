/*
 * Copyright 2014 Continuuity, Inc.
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

import co.cask.cdap.api.app.ApplicationConfigurer;

import java.util.Map;

/**
 *
 */
public abstract class AbstractHttpServiceHandler implements HttpServiceHandler {
  private HttpServiceConfigurer configurer;
  private HttpServiceContext context;

  public abstract void configure();

  @Override
  public final void configure(HttpServiceConfigurer configurer) {
    this.configurer = configurer;

    configure();
  }

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    this.context = context;
  }

  @Override
  public void destroy() {
    // nothing to do
  }

  /**
   * @return
   */
  protected final HttpServiceContext getContext() {
    return context;
  }

  /**
   * @return The {@link ApplicationConfigurer} used to configure the {@link co.cask.cdap.api.app.Application}
   */
  protected final HttpServiceConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * @see ApplicationConfigurer#setName(String)
   */
  protected final void setName(String name) {
    configurer.setName(name);
  }

  /**
   * @see ApplicationConfigurer#setDescription(String)
   */
  protected final void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   *
   * @param arguments
   */
  protected final void setArguments(Map<String, String> arguments) {
    configurer.setArguments(arguments);
  }
}
