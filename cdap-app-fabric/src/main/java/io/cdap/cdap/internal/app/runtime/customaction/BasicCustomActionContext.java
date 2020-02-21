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

package io.cdap.cdap.internal.app.runtime.customaction;

import io.cdap.cdap.api.ProgramState;
import io.cdap.cdap.api.customaction.CustomActionContext;
import io.cdap.cdap.api.customaction.CustomActionSpecification;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.runtime.AbstractContext;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.messaging.MessagingService;
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
                                  SecureStore secureStore, SecureStoreManager secureStoreManager,
                                  MessagingService messagingService, MetadataReader metadataReader,
                                  MetadataPublisher metadataPublisher, NamespaceQueryAdmin namespaceQueryAdmin,
                                  FieldLineageWriter fieldLineageWriter) {

    super(workflow, programOptions, cConf, customActionSpecification.getDatasets(),
          datasetFramework, txClient, discoveryServiceClient, false,
          metricsCollectionService, workflowProgramInfo.updateMetricsTags(new HashMap<>()), secureStore,
          secureStoreManager, messagingService, pluginInstantiator, metadataReader, metadataPublisher,
          namespaceQueryAdmin, fieldLineageWriter);

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
