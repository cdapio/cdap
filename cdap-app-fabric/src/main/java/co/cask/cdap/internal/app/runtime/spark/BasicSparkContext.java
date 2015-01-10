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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.stream.StreamEventDecoder;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.services.SerializableServiceDiscoverer;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.logging.context.SparkLoggingContext;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionAware;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Spark job runtime context. This context serves as the bridge between CDAP {@link SparkProgramRunner} and Spark
 * programs running in YARN containers. The {@link SparkProgramRunner} builds this context and writes it to {@link
 * Configuration} through {@link SparkContextConfig}. The running Spark jobs loads the {@link Configuration} files
 * which was packaged with the dependency jar when the job was submitted and constructs this context back through
 * {@link SparkContextProvider}. This allow Spark jobs running outside CDAP have access to Transaction,
 * start time and other stuff.
 */
public class BasicSparkContext extends AbstractContext implements SparkContext {

  private static final Logger LOG = LoggerFactory.getLogger(BasicSparkContext.class);
  private static final Pattern SPACES = Pattern.compile("\\s+");
  private static final String[] NO_ARGS = {};

  private File metricsPropertyFile;

  private final Map<MetricsScope, MetricsCollector> metricsCollectors;
  private final SparkSpecification sparkSpec;
  private final long logicalStartTime;
  private final String workflowBatch;
  private final StreamAdmin streamAdmin;
  private final SparkLoggingContext loggingContext;
  private final SerializableServiceDiscoverer serializableServiceDiscoverer;
  private final Metrics userMetrics;

  public void setMetricsPropertyFile(File file) {
    metricsPropertyFile = file;
  }

  public File getMetricsPropertyFile() {
    return metricsPropertyFile;
  }

  public BasicSparkContext(Program program, RunId runId, Arguments runtimeArguments, Set<String> datasets,
                           SparkSpecification sparkSpec, long logicalStartTime, String workflowBatch,
                           MetricsCollectionService metricsCollectionService,
                           DatasetFramework dsFramework, CConfiguration conf,
                           DiscoveryServiceClient discoveryServiceClient, StreamAdmin streamAdmin) {
    super(program, runId, runtimeArguments, datasets,
          getMetricCollector(metricsCollectionService, MetricsScope.SYSTEM, program, runId.getId()),
          dsFramework, conf, discoveryServiceClient);
    this.logicalStartTime = logicalStartTime;
    this.workflowBatch = workflowBatch;
    this.streamAdmin = streamAdmin;
    SerializableServiceDiscoverer.setDiscoveryServiceClient(getDiscoveryServiceClient());
    this.serializableServiceDiscoverer = new SerializableServiceDiscoverer(getProgram());
    this.metricsCollectors = Maps.newHashMap();
    for (MetricsScope scope : MetricsScope.values()) {
      // TODO: InstanceId is not supported in Spark jobs, see CDAP-39.
      this.metricsCollectors.put(
        scope, getMetricCollector(metricsCollectionService, scope, program, runId.getId()));
    }
    this.userMetrics = new ProgramUserMetrics(metricsCollectors.get(MetricsScope.USER));
    this.loggingContext = new SparkLoggingContext(getAccountId(), getApplicationId(), getProgramName());
    this.sparkSpec = sparkSpec;
  }

  /**
   * Returns a {@link Serializable} {@link ServiceDiscoverer} for Service Discovery in Spark Program which can be
   * passed in Spark program's closures.
   *
   * @return A {@link Serializable} {@link ServiceDiscoverer} which is {@link SerializableServiceDiscoverer}
   */
  public SerializableServiceDiscoverer getSerializableServiceDiscoverer() {
    return serializableServiceDiscoverer;
  }

  @Override
  public String toString() {
    return String.format("Job=%s: %s, %s", ProgramType.SPARK.name().toLowerCase(), sparkSpec.getName(),
                         super.toString());
  }

  @Override
  public SparkSpecification getSpecification() {
    return sparkSpec;
  }

  @Override
  public long getLogicalStartTime() {
    return logicalStartTime;
  }

  @Override
  public <T> T readFromDataset(String datasetName, Class<?> kClass, Class<?> vClass) {
    throw new IllegalStateException("Reading dataset is not supported here");
  }

  @Override
  public <T> void writeToDataset(T rdd, String datasetName, Class<?> kClass, Class<?> vClass) {
    throw new IllegalStateException("Writing  dataset is not supported here");
  }

  @Override
  public <T> T readFromStream(String streamName, Class<?> vClass) {
    throw new IllegalStateException("Reading stream is not supported here");
  }

  @Override
  public <T> T readFromStream(String streamName, Class<?> vClass, long startTime, long endTime) {
    throw new IllegalStateException("Reading stream is not supported here");
  }

  @Override
  public <T> T readFromStream(String streamName, Class<?> vClass, long startTime, long endTime,
                              Class<? extends StreamEventDecoder> decoderType) {
    throw new IllegalStateException("Reading stream is not supported here");
  }

  private static MetricsCollector getMetricCollector(MetricsCollectionService service,
                                                     MetricsScope scope, Program program, String runId) {
    if (service == null) {
      return null;
    }
    Map<String, String> tags = Maps.newHashMap(getMetricsContext(program, runId));
    return service.getCollector(scope, tags);
  }

  @Override
  public <T> T getOriginalSparkContext() {
    throw new IllegalStateException("Getting base Spark Context is not supported here");
  }

  /**
   * Returns value of the given argument key as a String[]
   *
   * @param argsKey {@link String} which is the key for the argument
   * @return String[] containing all the arguments which is indexed by their position as they were supplied
   */
  @Override
  public String[] getRuntimeArguments(String argsKey) {
    if (getRuntimeArguments().containsKey(argsKey)) {
      return SPACES.split(getRuntimeArguments().get(argsKey).trim());
    } else {
      LOG.warn("Argument with key {} not found in Runtime Arguments", argsKey);
      return NO_ARGS;
    }
  }

  @Override
  public ServiceDiscoverer getServiceDiscoverer() {
    throw new IllegalStateException("Service Discovery is not supported in this Context");
  }

  /**
   * @return the {@link StreamAdmin} to access Streams in Spark through {@link AbstractSparkContext}
   */
  public StreamAdmin getStreamAdmin() {
    return streamAdmin;
  }

  @Override
  public Metrics getMetrics() {
    return userMetrics;
  }

  /**
   * @return {@link LoggingContext} for the job which is {@link SparkLoggingContext}
   */
  public LoggingContext getLoggingContext() {
    return loggingContext;
  }

  /**
   * Request all txAwares guys to persist all the writes which was cached in memory.
   *
   * @throws Exception which is thrown by the commit on the {@link Dataset}
   */
  public void flushOperations() throws Exception {
    for (TransactionAware txAware : getDatasetInstantiator().getTransactionAware()) {
      txAware.commitTx();
    }
  }

  /**
   * @param scope {@link MetricsScope} of the {@link MetricsCollector}
   * @return the {@link MetricsCollector} for the given {@link MetricsScope}
   */
  public MetricsCollector getMetricsCollector(MetricsScope scope) {
    return metricsCollectors.get(scope);
  }
}
