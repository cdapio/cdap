/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.service.ServiceWorker;
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.service.BasicServiceWorkerContext;
import co.cask.cdap.internal.lang.Reflections;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

import java.util.concurrent.Executor;

/**
 * A guava Service for executing {@link ServiceWorker} logic.
 */
public class ServiceWorkerDriver extends AbstractExecutionThreadService {

  private final Program program;
  private final ServiceWorkerSpecification spec;
  private final BasicServiceWorkerContext context;

  private ServiceWorker serviceWorker;

  public ServiceWorkerDriver(Program program, ServiceWorkerSpecification spec, BasicServiceWorkerContext context) {
    this.program = program;
    this.spec = spec;
    this.context = context;
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

    // Instantiate worker instance
    Class<?> workerClass = program.getClassLoader().loadClass(spec.getClassName());
    @SuppressWarnings("unchecked")
    TypeToken<ServiceWorker> workerType = (TypeToken<ServiceWorker>) TypeToken.of(workerClass);
    serviceWorker = new InstantiatorFactory(false).get(workerType).create();

    // Fields injection
    Reflections.visit(serviceWorker, workerType,
                      new MetricsFieldSetter(context.getMetrics()),
                      new PropertyFieldSetter(spec.getProperties()));

    // Initialize worker
    initialize();
  }

  @Override
  protected void run() throws Exception {
    ClassLoader classLoader = ClassLoaders.setContextClassLoader(serviceWorker.getClass().getClassLoader());
    try {
      serviceWorker.run();
    } finally {
      ClassLoaders.setContextClassLoader(classLoader);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    if (serviceWorker == null) {
      return;
    }
    ClassLoader classLoader = ClassLoaders.setContextClassLoader(serviceWorker.getClass().getClassLoader());
    try {
      serviceWorker.destroy();
    } finally {
      ClassLoaders.setContextClassLoader(classLoader);
    }
  }

  @Override
  protected void triggerShutdown() {
    if (serviceWorker == null) {
      return;
    }
    ClassLoader classLoader = ClassLoaders.setContextClassLoader(serviceWorker.getClass().getClassLoader());
    try {
      serviceWorker.stop();
    } finally {
      ClassLoaders.setContextClassLoader(classLoader);
    }
  }

  @Override
  protected Executor executor() {
    return new Executor() {
      @Override
      public void execute(Runnable command) {
        Thread t = new Thread(command, String.format("service-worker-%s-%s", program.getName(), spec.getName()));
        t.setDaemon(true);
        t.start();
      }
    };
  }

  public void setInstanceCount(int instanceCount) {
    context.setInstanceCount(instanceCount);
  }

  private void initialize() throws Exception {
    ClassLoader classLoader = ClassLoaders.setContextClassLoader(serviceWorker.getClass().getClassLoader());
    try {
      serviceWorker.initialize(context);
    } finally {
      ClassLoaders.setContextClassLoader(classLoader);
    }
  }
}
