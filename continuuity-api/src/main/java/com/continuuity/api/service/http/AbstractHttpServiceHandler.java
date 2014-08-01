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

package com.continuuity.api.service.http;

import com.continuuity.api.app.ApplicationConfigurer;
import com.continuuity.api.app.ApplicationContext;

import java.util.Map;

/**
 *
 */
public abstract class AbstractHttpServiceHandler implements HttpServiceHandler {
  HttpServiceConfigurer configurer;
  HttpServiceContext context;

  public abstract void configure();

  @Override
  public final void configure(HttpServiceConfigurer configurer, HttpServiceContext context) {
    this.context = context;
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
  protected HttpServiceContext getContext() {
    return context;
  }

  /**
   * @return The {@link ApplicationConfigurer} used to configure the {@link com.continuuity.api.app.Application}
   */
  protected HttpServiceConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * @see ApplicationConfigurer#setName(String)
   */
  protected void setName(String name) {
    configurer.setName(name);
  }

  /**
   * @see ApplicationConfigurer#setDescription(String)
   */
  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   *
   * @param arguments
   */
  protected void setArguments(Map<String, String> arguments) {
    configurer.setArguments(arguments);
  }
}
