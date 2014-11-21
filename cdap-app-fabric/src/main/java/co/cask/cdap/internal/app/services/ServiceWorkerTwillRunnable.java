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

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.ServiceWorker;
import co.cask.cdap.api.service.ServiceWorkerSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.service.BasicServiceWorkerContext;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import org.apache.twill.api.Command;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * {@link TwillRunnable} to run a {@link ServiceWorker}.
 */
public class ServiceWorkerTwillRunnable implements TwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceWorkerTwillRunnable.class);
  private static final Gson GSON = new Gson();
  private ServiceWorker worker;
  private Program program;
  private RunId runId;
  private String runnableName;
  private ClassLoader programClassLoader;
  private TransactionSystemClient transactionSystemClient;
  private MetricsCollectionService metricsCollectionService;
  private DatasetFramework datasetFramework;
  private CConfiguration cConfiguration;
  private DiscoveryServiceClient discoveryServiceClient;
  private Set<String> datasets;
  private Metrics metrics;

  /**
   * Create a {@link TwillRunnable} for a {@link ServiceWorker}.
   * @param worker to run as runnable.
   */
  public ServiceWorkerTwillRunnable(ServiceWorker worker, Set<String> datasets) {
    this.worker = worker;
    this.datasets = datasets;
  }

  /**
   * Create a {@link TwillRunnable} for a {@link ServiceWorker} from a classloader.
   * @param classLoader to create runnable with.
   */
  public ServiceWorkerTwillRunnable(Program program, RunId runId, String runnableName, ClassLoader classLoader,
                                    CConfiguration cConfiguration,
                                    MetricsCollectionService metricsCollectionService,
                                    DatasetFramework datasetFramework,
                                    TransactionSystemClient transactionSystemClient,
                                    DiscoveryServiceClient discoveryServiceClient) {
    this.program = program;
    this.runId = runId;
    this.runnableName = runnableName;
    this.programClassLoader = classLoader;
    this.transactionSystemClient = transactionSystemClient;
    this.metricsCollectionService = metricsCollectionService;
    this.datasetFramework = datasetFramework;
    this.cConfiguration = cConfiguration;
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public TwillRunnableSpecification configure() {
    ServiceWorkerSpecification workerSpecification = worker.configure();
    Map<String, String> runnableArgs = Maps.newHashMap(workerSpecification.getProperties());
    if (worker instanceof GuavaServiceWorker) {
      runnableArgs.put("delegate.class.name", ((GuavaServiceWorker) worker).getDelegate().getClass().getName());
    }
    runnableArgs.put("service.class.name", workerSpecification.getClassName());

    // Serialize and store the datasets that have explicitly been granted access to.
    String serializedDatasets = GSON.toJson(datasets, new TypeToken<Set<String>>() { }.getType());
    runnableArgs.put("service.datasets", serializedDatasets);
    return TwillRunnableSpecification.Builder.with()
                                             .setName(worker.getClass().getSimpleName())
                                             .withConfigs(runnableArgs)
                                             .build();
  }

  @Override
  public void initialize(TwillContext context) {
    Map<String, String> runnableArgs = context.getSpecification().getConfigs();
    String serviceClassName = runnableArgs.get("service.class.name");
    datasets = GSON.fromJson(runnableArgs.get("service.datasets"), new TypeToken<Set<String>>() { }.getType());

    InstantiatorFactory factory = new InstantiatorFactory(false);
    try {
      TypeToken<?> type = TypeToken.of(programClassLoader.loadClass(serviceClassName));
      worker = (ServiceWorker) factory.get(type).create();
      Reflections.visit(worker, type, new MetricsFieldSetter(metrics),
                                      new PropertyFieldSetter(runnableArgs));
      if (worker instanceof GuavaServiceWorker) {
        String delegateClassName = runnableArgs.get("delegate.class.name");
        type = TypeToken.of(programClassLoader.loadClass(delegateClassName));
        ((GuavaServiceWorker) worker).setDelegate((Service) factory.get(type).create());
      }
      int instanceId = context.getInstanceId();
      worker.initialize(new BasicServiceWorkerContext(program, runId, instanceId, runnableName, programClassLoader,
                                                      cConfiguration, context.getSpecification().getConfigs(), datasets,
                                                      metricsCollectionService, datasetFramework,
                                                      transactionSystemClient,
                                                      discoveryServiceClient));
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
