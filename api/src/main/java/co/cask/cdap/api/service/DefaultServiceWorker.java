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

import com.google.common.base.Throwables;
import org.apache.twill.api.Command;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;

import java.util.Map;

/**
 * Extend this class to add workers to a custom Service.
 */
public abstract class DefaultServiceWorker implements ServiceWorker, TwillRunnable {
  private final String name;
  private final String description;
  private Map<String, String> args;

  @Override
  public ServiceWorkerSpecification configure(ServiceWorkerContext context) {
    this.args = context.getRuntimeArguments();
    return new DefaultServiceWorkerSpecification(getClass().getSimpleName(), name, description, args);
  }

  /**
   * Create a ServiceWorker with no runtime arguments.
   */
  public DefaultServiceWorker(String name, String description, Map<String, String> runtimeArgs) {
    this.name = name;
    this.description = description;
    this.args = runtimeArgs;
  }

  @Override
  public final TwillRunnableSpecification configure() {
    return TwillRunnableSpecification.Builder.with()
      .setName(getClass().getSimpleName())
      .withConfigs(args)
      .build();
  }

  @Override
  public final void initialize(TwillContext context) {
    try {
      initialize(new DefaultServiceWorkerContext(context.getSpecification().getConfigs()));
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  @Override
  public final void handleCommand(Command command) throws Exception {
    // no-op
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return this.args;
  }
}
