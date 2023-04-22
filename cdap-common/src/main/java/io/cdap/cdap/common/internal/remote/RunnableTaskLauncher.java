/*
 * Copyright © 2021-2023 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.api.service.worker.RunnableTaskContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * RunnableTaskLauncher launches a {@link RunnableTask} by loading its class and calling its run
 * method.
 */
public class RunnableTaskLauncher {

  private final Injector injector;

  public RunnableTaskLauncher(CConfiguration cConf,
      DiscoveryService discoveryService,
      DiscoveryServiceClient discoveryServiceClient,
      MetricsCollectionService metricsCollectionService) {
    injector = Guice.createInjector(
        new ConfigModule(cConf),
        new RunnableTaskModule(discoveryService, discoveryServiceClient,
            metricsCollectionService)
    );
  }

  /**
   * Returns a {@link RunnableTaskLauncher} using an Injector. This is used to launch a {@link
   * RunnableTask} using the calling service's guice bindings.
   */
  public RunnableTaskLauncher(Injector injector) {
    this.injector = injector;
  }

  public void launchRunnableTask(RunnableTaskContext context) throws Exception {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = getClass().getClassLoader();
    }

    Class<?> clazz = classLoader.loadClass(context.getClassName());

    Object obj = injector.getInstance(clazz);

    if (!(obj instanceof RunnableTask)) {
      throw new ClassCastException(
          String.format("%s is not a RunnableTask", context.getClassName()));
    }
    RunnableTask runnableTask = (RunnableTask) obj;
    runnableTask.run(context);
  }
}
