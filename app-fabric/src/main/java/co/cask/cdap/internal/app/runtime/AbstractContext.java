/*
 * Copyright 2014 Cask Data, Inc.
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

import co.cask.cdap.api.data.DataSetContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.data.dataset.DataSetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import com.google.common.base.Preconditions;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

/**
 * Base class for program runtime context
 */
public abstract class AbstractContext implements DataSetContext {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractContext.class);

  private final Program program;
  private final RunId runId;
  private final Map<String, Closeable> datasets;

  private final MetricsCollector programMetrics;

  private final DataSetInstantiator dsInstantiator;

  public AbstractContext(Program program, RunId runId,
                         Set<String> datasets,
                         String metricsContext,
                         MetricsCollectionService metricsCollectionService,
                         DatasetFramework dsFramework,
                         CConfiguration conf) {
    this.program = program;
    this.runId = runId;

    MetricsCollector datasetMetrics;
    if (metricsCollectionService != null) {
      // NOTE: RunId metric is not supported now. Need UI refactoring to enable it.
      this.programMetrics = metricsCollectionService.getCollector(MetricsScope.REACTOR, metricsContext, "0");
      datasetMetrics = metricsCollectionService.getCollector(MetricsScope.REACTOR,
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
