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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.data.DataSetContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.data.dataset.DataSetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import com.google.common.base.Preconditions;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Base class for program runtime context
 */
public abstract class AbstractContext implements DataSetContext, RuntimeContext {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractContext.class);

  private final Program program;
  private final RunId runId;
  private final Map<String, Closeable> datasets;

  private final MetricsCollector programMetrics;

  private final DataSetInstantiator dsInstantiator;

  private final DiscoveryServiceClient discoveryServiceClient;

  public AbstractContext(Program program, RunId runId,
                         Set<String> datasets,
                         String metricsContext,
                         MetricsCollectionService metricsCollectionService,
                         DatasetFramework dsFramework,
                         CConfiguration conf,
                         DiscoveryServiceClient discoveryServiceClient) {
    this.program = program;
    this.runId = runId;
    this.discoveryServiceClient = discoveryServiceClient;

    MetricsCollector datasetMetrics;
    if (metricsCollectionService != null) {
      // NOTE: RunId metric is not supported now. Need UI refactoring to enable it.
      this.programMetrics = metricsCollectionService.getCollector(MetricsScope.SYSTEM, metricsContext, "0");
      datasetMetrics = metricsCollectionService.getCollector(MetricsScope.SYSTEM,
                                                             Constants.Metrics.DATASET_CONTEXT, "0");
    } else {
      this.programMetrics = null;
      datasetMetrics = null;
    }

    this.dsInstantiator = new DataSetInstantiator(dsFramework, conf, program.getClassLoader(),
                                                  datasetMetrics, programMetrics);

    // todo: this should be instantiated on demand, at run-time dynamically. Esp. bad to do that in ctor...
    // todo: initialized datasets should be managed by DatasetContext (ie. DatasetInstantiator): refactor further
    this.datasets = DataSets.createDataSets(dsInstantiator, datasets);
  }

  public abstract Metrics getMetrics();

  @Override
  public String toString() {
    return String.format("accountId=%s, applicationId=%s, program=%s, runid=%s",
                         getAccountId(), getApplicationId(), getProgramName(), runId);
  }

  public MetricsCollector getProgramMetrics() {
    return programMetrics;
  }

  // todo: this may be refactored further: avoid leaking dataset instantiator from context
  public DataSetInstantiator getDatasetInstantiator() {
    return dsInstantiator;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Closeable> T getDataSet(String name) {
    // TODO this should allow to get a dataset that was not declared with @UseDataSet. Then we can support arguments.
    T dataSet = (T) datasets.get(name);
    Preconditions.checkArgument(dataSet != null, "%s is not a known DataSet.", name);
    return dataSet;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Closeable> T getDataSet(String name, Map<String, String> arguments) {
    // TODO this should allow to get a dataset that was not declared with @UseDataSet. Then we can support arguments.
    T dataSet = (T) datasets.get(name);
    Preconditions.checkArgument(dataSet != null, "%s is not a known DataSet.", name);
    return dataSet;
  }

  public String getAccountId() {
    return program.getAccountId();
  }

  public String getApplicationId() {
    return program.getApplicationId();
  }

  public String getProgramName() {
    return program.getName();
  }

  public Program getProgram() {
    return program;
  }

  public RunId getRunId() {
    return runId;
  }

  @Override
  public URL getServiceURL(final String applicationId, final String serviceId) {
    ServiceDiscovered serviceDiscovered = discoveryServiceClient.discover(String.format("service.%s.%s.%s",
                                                                                        getAccountId(),
                                                                                        applicationId,
                                                                                        serviceId));
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(serviceDiscovered);
    Discoverable discoverable = endpointStrategy.pick();
    if (discoverable != null) {
      return createURL(discoverable, applicationId, serviceId);
    }

    final SynchronousQueue<URL> discoverableQueue = new SynchronousQueue<URL>();
    Cancellable discoveryCancel = serviceDiscovered.watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        try {
          URL url = createURL(serviceDiscovered.iterator().next(), applicationId, serviceId);
          discoverableQueue.offer(url);
        } catch (NoSuchElementException e) {
          LOG.debug("serviceDiscovered is empty");
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    try {
      URL url = discoverableQueue.poll(1, TimeUnit.SECONDS);
      if (url == null) {
        LOG.debug("Discoverable endpoint not found for appID: {}, serviceID: {}.", applicationId, serviceId);
      }
      return url;
    } catch (InterruptedException e) {
      LOG.error("Got exception: ", e);
      return null;
    } finally {
      discoveryCancel.cancel();
    }
  }

  @Override
  public URL getServiceURL(String serviceId) {
    return getServiceURL(getApplicationId(), serviceId);
  }

  private URL createURL(@Nullable Discoverable discoverable, String applicationId, String serviceId) {
    if (discoverable == null) {
      return null;
    }
    String hostName = discoverable.getSocketAddress().getHostName();
    int port = discoverable.getSocketAddress().getPort();
    String path = String.format("http://%s:%d%s/apps/%s/services/%s/methods/", hostName, port,
                                Constants.Gateway.GATEWAY_VERSION, applicationId, serviceId);
    try {
      return new URL(path);
    } catch (MalformedURLException e) {
      LOG.error("Got exception while creating serviceURL", e);
      return null;
    }
  }

  /**
   * Release all resources held by this context, for example, datasets. Subclasses should override this
   * method to release additional resources.
   */
  public void close() {
    for (Closeable ds : datasets.values()) {
      closeDataSet(ds);
    }
  }

  /**
   * Closes one dataset; logs but otherwise ignores exceptions.
   */
  protected void closeDataSet(Closeable ds) {
    try {
      ds.close();
    } catch (Throwable t) {
      LOG.error("Dataset throws exceptions during close:" + ds.toString() + ", in context: " + this);
    }
  }
}
