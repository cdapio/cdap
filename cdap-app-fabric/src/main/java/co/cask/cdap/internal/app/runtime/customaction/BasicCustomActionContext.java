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
import co.cask.cdap.api.customaction.CustomActionContext;
import co.cask.cdap.api.customaction.CustomActionSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.HashMap;
import javax.annotation.Nullable;

/**
 * Implementation of {@link CustomActionContext}.
 */
public class BasicCustomActionContext extends AbstractContext implements CustomActionContext {

  private final CustomActionSpecification customActionSpecification;
  private final WorkflowProgramInfo workflowProgramInfo;
  private ProgramState state;

  public BasicCustomActionContext(Program workflow, ProgramOptions programOptions, CConfiguration cConf,
                                  CustomActionSpecification customActionSpecification,
                                  WorkflowProgramInfo workflowProgramInfo,
                                  MetricsCollectionService metricsCollectionService,
                                  DatasetFramework datasetFramework, TransactionSystemClient txClient,
                                  DiscoveryServiceClient discoveryServiceClient,
                                  @Nullable PluginInstantiator pluginInstantiator,
                                  SecureStore secureStore, SecureStoreManager secureStoreManager) {

    super(workflow, programOptions, cConf, customActionSpecification.getDatasets(),
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
  public WorkflowToken getWorkflowToken() {
    return workflowProgramInfo.getWorkflowToken();
  }

  @Override
  public WorkflowProgramInfo getWorkflowInfo() {
    return workflowProgramInfo;
  }
}
