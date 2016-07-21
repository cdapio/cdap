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
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Throwables;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import javax.annotation.Nullable;

/**
 * Implementation of {@link CustomActionContext}.
 */
public class BasicCustomActionContext extends AbstractContext implements CustomActionContext {

  private static final Logger LOG = LoggerFactory.getLogger(BasicCustomActionContext.class);
  private final CustomActionSpecification customActionSpecification;
  private final WorkflowProgramInfo workflowProgramInfo;
  private ProgramState state;

  public BasicCustomActionContext(Program workflow, ProgramOptions programOptions,
                                  CustomActionSpecification customActionSpecification,
                                  WorkflowProgramInfo workflowProgramInfo,
                                  MetricsCollectionService metricsCollectionService,
                                  DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                  DiscoveryServiceClient discoveryServiceClient,
                                  @Nullable PluginInstantiator pluginInstantiator,
                                  SecureStore secureStore, SecureStoreManager secureStoreManager) {

    super(workflow, programOptions, customActionSpecification.getDatasets(),
          datasetFramework, txClient, discoveryServiceClient, false,
          metricsCollectionService, workflowProgramInfo.updateMetricsTags(new HashMap<String, String>()), secureStore,
          secureStoreManager, pluginInstantiator);

    this.customActionSpecification = customActionSpecification;
    this.workflowProgramInfo = workflowProgramInfo;
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
