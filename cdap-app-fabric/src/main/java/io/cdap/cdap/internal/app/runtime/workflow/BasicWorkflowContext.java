/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.workflow;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.ProgramState;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.workflow.ConditionSpecification;
import io.cdap.cdap.api.workflow.WorkflowContext;
import io.cdap.cdap.api.workflow.WorkflowNodeState;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.runtime.AbstractContext;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.messaging.MessagingService;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation of a {@link WorkflowContext}.
 */
final class BasicWorkflowContext extends AbstractContext implements WorkflowContext {

  private final WorkflowSpecification workflowSpec;
  private final ConditionSpecification conditionSpecification;
  private final WorkflowToken token;
  private final Map<String, WorkflowNodeState> nodeStates;
  private ProgramState state;
  private boolean consolidateFieldOperations;

  BasicWorkflowContext(WorkflowSpecification workflowSpec,
                       WorkflowToken token, Program program, ProgramOptions programOptions, CConfiguration cConf,
                       MetricsCollectionService metricsCollectionService,
                       DatasetFramework datasetFramework, TransactionSystemClient txClient,
                       DiscoveryServiceClient discoveryServiceClient, Map<String, WorkflowNodeState> nodeStates,
                       @Nullable PluginInstantiator pluginInstantiator,
                       SecureStore secureStore, SecureStoreManager secureStoreManager,
                       MessagingService messagingService, @Nullable ConditionSpecification conditionSpecification,
                       MetadataReader metadataReader, MetadataPublisher metadataPublisher,
                       NamespaceQueryAdmin namespaceQueryAdmin, FieldLineageWriter fieldLineageWriter) {
    super(program, programOptions, cConf, new HashSet<>(),
          datasetFramework, txClient, discoveryServiceClient, false,
          metricsCollectionService, Collections.singletonMap(Constants.Metrics.Tag.WORKFLOW_RUN_ID,
                                                             ProgramRunners.getRunId(programOptions).getId()),
          secureStore, secureStoreManager, messagingService, pluginInstantiator, metadataReader, metadataPublisher,
          namespaceQueryAdmin, fieldLineageWriter);
    this.workflowSpec = workflowSpec;
    this.conditionSpecification = conditionSpecification;
    this.token = token;
    this.nodeStates = nodeStates;
  }

  @Override
  public WorkflowSpecification getWorkflowSpecification() {
    return workflowSpec;
  }

  @Override
  public ConditionSpecification getConditionSpecification() {
    if (conditionSpecification == null) {
      throw new UnsupportedOperationException("Operation not allowed.");
    }
    return conditionSpecification;
  }

  @Override
  public WorkflowToken getToken() {
    return token;
  }

  @Override
  public Map<String, WorkflowNodeState> getNodeStates() {
    return ImmutableMap.copyOf(nodeStates);
  }

  /**
   * Sets the current state of the program.
   */
  void setState(ProgramState state) {
    this.state = state;
  }

  @Override
  public ProgramState getState() {
    return state;
  }

  boolean fieldLineageConsolidationEnabled() {
    return this.consolidateFieldOperations;
  }

  @Override
  public void enableFieldLineageConsolidation() {
    this.consolidateFieldOperations = true;
  }
}
