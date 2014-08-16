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

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Extend this class to add workers to a custom Service.
 */
public abstract class AbstractServiceWorker implements ServiceWorker {
  protected String name;
  protected String description;
  protected Map<String, String> args;

  /**
   * Default constructor used to instantiate a ServiceWorker.
   */
  public AbstractServiceWorker() {
    this.name = "";
    this.description = "";
    this.args = Maps.newHashMap();
  }

  /**
   * Create a ServiceWorker with no runtime arguments.
   */
  public AbstractServiceWorker(String name, String description, Map<String, String> runtimeArgs) {
    this.name = name;
    this.description = description;
    this.args = runtimeArgs;
  }

  @Override
  public ServiceWorkerSpecification configure() {
    return new DefaultServiceWorkerSpecification(getClass().getName(), name, description, args);
  }

  @Override
  public void initialize(ServiceWorkerContext context) throws Exception {

  }
}
