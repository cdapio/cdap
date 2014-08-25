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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.ServiceWorker;
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.service.DefaultServiceWorkerContext;
import co.cask.cdap.internal.lang.Reflections;
import com.continuuity.tephra.TransactionSystemClient;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import org.apache.twill.api.Command;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * {@link TwillRunnable} to run a {@link ServiceWorker}.
 */
public class ServiceWorkerTwillRunnable implements TwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceWorkerTwillRunnable.class);
  private ServiceWorker worker;
  private ClassLoader programClassLoader;
  private TransactionSystemClient transactionSystemClient;
  private DatasetFramework datasetFramework;
  private Metrics metrics;

  /**
   * Create a {@link TwillRunnable} for a {@link ServiceWorker}.
   * @param worker to run as runnable.
   */
  public ServiceWorkerTwillRunnable(ServiceWorker worker) {
    this.worker = worker;
  }

  /**
   * Create a {@link TwillRunnable} for a {@link ServiceWorker} from a classloader.
   * @param classLoader to create runnable with.
   */
  public ServiceWorkerTwillRunnable(ClassLoader classLoader, TransactionSystemClient transactionSystemClient,
                                    DatasetFramework datasetFramework) {
    this.programClassLoader = classLoader;
    this.transactionSystemClient = transactionSystemClient;
    this.datasetFramework = datasetFramework;
  }

  @Override
  public TwillRunnableSpecification configure() {
    ServiceWorkerSpecification workerSpecification = worker.configure();
    Map<String, String> runnableArgs = Maps.newHashMap(workerSpecification.getProperties());
    runnableArgs.put("service.class.name", workerSpecification.getClassName());
    return TwillRunnableSpecification.Builder.with()
                                             .setName(worker.getClass().getSimpleName())
                                             .withConfigs(runnableArgs)
                                             .build();
  }

  @Override
  public void initialize(TwillContext context) {
    Map<String, String> runnableArgs = context.getSpecification().getConfigs();
    String serviceClassName = runnableArgs.get("service.class.name");
    InstantiatorFactory factory = new InstantiatorFactory(false);
    try {
      TypeToken<?> type = TypeToken.of(programClassLoader.loadClass(serviceClassName));
      worker = (ServiceWorker) factory.get(type).create();
      Reflections.visit(worker, type, new MetricsFieldSetter(metrics),
                                      new PropertyFieldSetter(runnableArgs));
      worker.initialize(new DefaultServiceWorkerContext(context.getSpecification().getConfigs(),
                                                        transactionSystemClient, datasetFramework));
    } catch (Exception e) {
      LOG.error("Could not instantiate service " + serviceClassName);
      Throwables.propagate(e);
    }
    LOG.info("Instantiated service " + serviceClassName);
  }

  @Override
  public final void handleCommand(Command command) throws Exception {
    // no-op
  }

  @Override
  public void stop() {
    worker.stop();
  }

  @Override
  public void destroy() {
    worker.destroy();
  }

  @Override
  public void run() {
    worker.run();
  }
}
