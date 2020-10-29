/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.WeakReferenceDelegatorClassLoader;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.runtime.AbstractContext;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;

import java.io.Closeable;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Context to be used at Spark runtime to provide common functionality that are needed at both the driver and
 * the executors.
 */
public final class SparkRuntimeContext extends AbstractContext implements Metrics {

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final String hostname;
  private final TransactionSystemClient txClient;
  private final DatasetFramework datasetFramework;
  private final WorkflowProgramInfo workflowProgramInfo;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;
  private final ServiceAnnouncer serviceAnnouncer;
  private final PluginFinder pluginFinder;
  private final LocationFactory locationFactory;
  private final Closeable closeable;

  // This is needed to maintain a strong reference while the Spark program is running,
  // since outside of this class, the spark classloader is wrapped with a WeakReferenceDelegatorClassLoader
  @SuppressWarnings("unused")
  private SparkClassLoader sparkClassLoader;

  SparkRuntimeContext(Configuration hConf, Program program, ProgramOptions programOptions,
                      CConfiguration cConf, String hostname, TransactionSystemClient txClient,
                      DatasetFramework datasetFramework,
                      DiscoveryServiceClient discoveryServiceClient,
                      MetricsCollectionService metricsCollectionService,
                      @Nullable WorkflowProgramInfo workflowProgramInfo,
                      @Nullable PluginInstantiator pluginInstantiator,
                      SecureStore secureStore,
                      SecureStoreManager secureStoreManager,
                      AuthorizationEnforcer authorizationEnforcer,
                      AuthenticationContext authenticationContext,
                      MessagingService messagingService, ServiceAnnouncer serviceAnnouncer,
                      PluginFinder pluginFinder, LocationFactory locationFactory,
                      MetadataReader metadataReader, MetadataPublisher metadataPublisher,
                      NamespaceQueryAdmin namespaceQueryAdmin, FieldLineageWriter fieldLineageWriter,
                      Closeable closeable) {
    super(program, programOptions, cConf, getSparkSpecification(program).getDatasets(), datasetFramework, txClient,
          discoveryServiceClient, true, metricsCollectionService, createMetricsTags(workflowProgramInfo),
          secureStore, secureStoreManager, messagingService, pluginInstantiator, metadataReader, metadataPublisher,
          namespaceQueryAdmin, fieldLineageWriter);
    this.cConf = cConf;
    this.hConf = hConf;
    this.hostname = hostname;
    this.txClient = txClient;
    this.datasetFramework = datasetFramework;
    this.workflowProgramInfo = workflowProgramInfo;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
    this.serviceAnnouncer = serviceAnnouncer;
    this.pluginFinder = pluginFinder;
    this.locationFactory = locationFactory;
    this.closeable = closeable;
  }

  @Override
  public void close() {
    super.close();
    Closeables.closeQuietly(closeable);
  }

  @Override
  public void count(String metricName, int delta) {
    getMetrics().count(metricName, delta);
  }

  @Override
  public void gauge(String metricName, long value) {
    getMetrics().gauge(metricName, value);
  }

  @Override
  public Metrics child(Map<String, String> tags) {
    return getMetrics().child(tags);
  }

  @Override
  public Map<String, String> getTags() {
    return getMetrics().getTags();
  }

  /**
   * Returns the {@link SparkSpecification} of the spark program of this context.
   */
  public SparkSpecification getSparkSpecification() {
    return getSparkSpecification(getProgram());
  }

  /**
   * Returns the hostname of the current container.
   */
  public String getHostname() {
    return hostname;
  }

  private static SparkSpecification getSparkSpecification(Program program) {
    SparkSpecification spec = program.getApplicationSpecification().getSpark().get(program.getName());
    // Spec shouldn't be null, otherwise the spark program won't even get started
    Preconditions.checkState(spec != null, "SparkSpecification not found for %s", program.getId());
    return spec;
  }

  /**
   * Returns the {@link WorkflowProgramInfo} if the spark program is running inside a workflow.
   */
  @Nullable
  public WorkflowProgramInfo getWorkflowInfo() {
    return workflowProgramInfo;
  }

  /**
   * Returns the {@link TransactionSystemClient} for this execution.
   */
  public TransactionSystemClient getTransactionSystemClient() {
    return txClient;
  }

  /**
   * Returns the CDAP {@link CConfiguration} used for the execution.
   */
  public CConfiguration getCConfiguration() {
    return cConf;
  }

  /**
   * Returns the {@link Configuration} used for the execution.
   */
  public Configuration getConfiguration() {
    return hConf;
  }

  /**
   * Returns the {@link DatasetFramework} used for this execution.
   */
  public DatasetFramework getDatasetFramework() {
    return datasetFramework;
  }

  /**
   * Returns the {@link AuthorizationEnforcer} that can be used for this program.
   */
  public AuthorizationEnforcer getAuthorizationEnforcer() {
    return authorizationEnforcer;
  }

  /**
   * Returns the {@link AuthenticationContext} that can be used for this program.
   */
  public AuthenticationContext getAuthenticationContext() {
    return authenticationContext;
  }

  /**
   * Returns the {@link ServiceAnnouncer} for announcing discoverables.
   */
  public ServiceAnnouncer getServiceAnnouncer() {
    return serviceAnnouncer;
  }

  /**
   * Returns the {@link PluginFinder} for locating plugins.
   */
  public PluginFinder getPluginFinder() {
    return pluginFinder;
  }

  /**
   * Returns the {@link LocationFactory} for the runtime environement.
   */
  public LocationFactory getLocationFactory() {
    return locationFactory;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(SparkRuntimeContext.class)
      .add("id", getProgram().getId())
      .add("runId", getRunId())
      .toString();
  }

  @Override
  protected ClassLoader createProgramInvocationClassLoader() {
    sparkClassLoader = new SparkClassLoader(this);
    ClassLoader classLoader = new WeakReferenceDelegatorClassLoader(sparkClassLoader);
    hConf.setClassLoader(classLoader);
    return classLoader;
  }

  /**
   * Creates metrics tags to be used for the Spark execution.
   */
  private static Map<String, String> createMetricsTags(@Nullable WorkflowProgramInfo workflowProgramInfo) {
    Map<String, String> tags = Maps.newHashMap();

    // todo: use proper spark instance id. For now we have to emit smth for test framework's waitFor metric to work
    tags.put(Constants.Metrics.Tag.INSTANCE_ID, "0");

    if (workflowProgramInfo != null) {
      workflowProgramInfo.updateMetricsTags(tags);
    }
    return tags;
  }
}
