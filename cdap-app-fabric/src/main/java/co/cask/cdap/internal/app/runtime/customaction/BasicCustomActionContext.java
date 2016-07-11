/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.customaction;

import co.cask.cdap.api.ProgramState;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.customaction.CustomActionContext;
import co.cask.cdap.api.customaction.CustomActionSpecification;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.program.ProgramTypeMetricTag;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of {@link CustomActionContext}.
 */
public class BasicCustomActionContext extends AbstractContext implements CustomActionContext {

  private static final Logger LOG = LoggerFactory.getLogger(BasicCustomActionContext.class);
  private final CustomActionSpecification customActionSpecification;
  private final WorkflowProgramInfo workflowProgramInfo;
  private final Metrics userMetrics;
  private ProgramState state;

  public BasicCustomActionContext(Program workflow, RunId runId, Arguments runtimeArguments,
                                  CustomActionSpecification customActionSpecification,
                                  WorkflowProgramInfo workflowProgramInfo,
                                  MetricsCollectionService metricsCollectionService,
                                  DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                  DiscoveryServiceClient discoveryServiceClient,
                                  @Nullable PluginInstantiator pluginInstantiator) {

    super(workflow, runId, runtimeArguments, customActionSpecification.getDatasets(),
          getMetricCollector(workflow, runId.getId(), metricsCollectionService), datasetFramework, txClient,
          discoveryServiceClient, false, pluginInstantiator);
    this.customActionSpecification = customActionSpecification;
    this.workflowProgramInfo = workflowProgramInfo;
    if (metricsCollectionService != null) {
      this.userMetrics = new ProgramUserMetrics(getProgramMetrics());
    } else {
      this.userMetrics = null;
    }
  }

  @Nullable
  private static MetricsContext getMetricCollector(Program program, String runId,
                                                   @Nullable MetricsCollectionService service) {
    if (service == null) {
      return null;
    }
    Map<String, String> tags = Maps.newHashMap();
    tags.put(Constants.Metrics.Tag.NAMESPACE, program.getNamespaceId());
    tags.put(Constants.Metrics.Tag.APP, program.getApplicationId());
    tags.put(ProgramTypeMetricTag.getTagName(program.getType()), program.getName());
    tags.put(Constants.Metrics.Tag.WORKFLOW_RUN_ID, runId);
    return service.getContext(tags);
  }

  @Override
  public Metrics getMetrics() {
    return userMetrics;
  }

  @Override
  public Map<String, Plugin> getPlugins() {
    return null;
  }

  @Override
  public CustomActionSpecification getSpecification() {
    return customActionSpecification;
  }

  /**
   * Sets the current state of the program.
   */
  public void setState(ProgramState state) {
    this.state = state;
  }

  @Override
  public ProgramState getState() {
    return state;
  }

  @Override
  public void execute(TxRunnable runnable) throws TransactionFailureException {
    final TransactionContext context = datasetCache.newTransactionContext();
    try {
      context.start();
      runnable.run(datasetCache);
      context.finish();
    } catch (TransactionFailureException e) {
      abortTransaction(e, "Failed to commit. Aborting transaction.", context);
    } catch (Exception e) {
      abortTransaction(e, "Exception occurred running user code. Aborting transaction.", context);
    }
  }

  private void abortTransaction(Exception e, String message, TransactionContext context) {
    try {
      LOG.error(message, e);
      context.abort();
      throw Throwables.propagate(e);
    } catch (TransactionFailureException e1) {
      LOG.error("Failed to abort transaction.", e1);
      throw Throwables.propagate(e1);
    }
  }

  @Override
  public WorkflowToken getWorkflowToken() {
    return workflowProgramInfo.getWorkflowToken();
  }

  @Override
  public WorkflowProgramInfo getWorkflowInfo() {
    return workflowProgramInfo;
  }
}
