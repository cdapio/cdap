/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowConditionNode;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowNodeType;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.verification.Verifier;
import co.cask.cdap.app.verification.VerifyResult;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.verification.ApplicationVerification;
import co.cask.cdap.internal.app.verification.DatasetCreationSpecVerifier;
import co.cask.cdap.internal.app.verification.FlowVerification;
import co.cask.cdap.internal.app.verification.ProgramVerification;
import co.cask.cdap.internal.app.verification.StreamVerification;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.internal.schedule.StreamSizeSchedule;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.SecurityUtil;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for verifying
 * the specification and components of specification. Verification of each
 * component of specification is achieved by the {@link Verifier}
 * concrete implementations.
 */
public class ApplicationVerificationStage extends AbstractStage<ApplicationDeployable> {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationVerificationStage.class);
  private final Map<Class<?>, Verifier<?>> verifiers = Maps.newIdentityHashMap();
  private final DatasetFramework dsFramework;
  private final Store store;
  private final OwnerAdmin ownerAdmin;

  public ApplicationVerificationStage(Store store, DatasetFramework dsFramework,
                                      OwnerAdmin ownerAdmin) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.store = store;
    this.dsFramework = dsFramework;
    this.ownerAdmin = ownerAdmin;
  }

  /**
   * Receives an input containing application specification and location
   * and verifies both.
   *
   * @param input An instance of {@link ApplicationDeployable}
   */
  @Override
  public void process(ApplicationDeployable input) throws Exception {
    Preconditions.checkNotNull(input);

    ApplicationSpecification specification = input.getSpecification();
    ApplicationId appId = input.getApplicationId();

    // verify that the owner principal is valid if one was given
    if (input.getOwnerPrincipal() != null) {
      SecurityUtil.validateKerberosPrincipal(input.getOwnerPrincipal());
    }

    Collection<ApplicationId> allAppVersionsAppIds = store.getAllAppVersionsAppIds(appId);
    // if allAppVersionsAppIds.isEmpty() is false that means some version of this app already exists so we should
    // verify that the owner is same
    if (!allAppVersionsAppIds.isEmpty()) {
      verifyOwner(appId, input.getOwnerPrincipal());
    }

    verifySpec(appId, specification);
    // We are verifying owner of dataset/stream at this stage itself even though the creation will fail in later
    // stage if the owner is different because we don't want to end up in scenario where we created few dataset/streams
    // and the failed because some dataset/stream already exists and have different owner
    verifyData(appId, specification, input.getOwnerPrincipal());
    verifyPrograms(appId, specification);

    // Emit the input to next stage.
    emit(input);
  }

  protected void verifySpec(ApplicationId appId,
                            ApplicationSpecification specification) {
    VerifyResult result = getVerifier(ApplicationSpecification.class).verify(appId, specification);
    if (!result.isSuccess()) {
      throw new RuntimeException(result.getMessage());
    }
  }

  protected void verifyData(ApplicationId appId,
                            ApplicationSpecification specification,
                            @Nullable KerberosPrincipalId specifiedOwnerPrincipal)
    throws DatasetManagementException, UnauthorizedException {
    // NOTE: no special restrictions on dataset module names, etc
    VerifyResult result;
    for (DatasetCreationSpec dataSetCreateSpec : specification.getDatasets().values()) {
      result = getVerifier(DatasetCreationSpec.class).verify(appId, dataSetCreateSpec);
      if (!result.isSuccess()) {
        throw new RuntimeException(result.getMessage());
      }
      String dsName = dataSetCreateSpec.getInstanceName();
      DatasetId datasetInstanceId = appId.getParent().dataset(dsName);
      DatasetSpecification existingSpec = dsFramework.getDatasetSpec(datasetInstanceId);
      if (existingSpec != null && !existingSpec.getType().equals(dataSetCreateSpec.getTypeName())) {
        // New app trying to deploy an dataset with same instanceName but different Type than that of existing.
        throw new DataSetException
          (String.format("Cannot Deploy Dataset : %s with Type : %s : Dataset with different Type Already Exists",
                         dsName, dataSetCreateSpec.getTypeName()));
      }

      // if the dataset existed verify its owner is same.
      if (existingSpec != null) {
        verifyOwner(datasetInstanceId, specifiedOwnerPrincipal);
      }
    }

    for (StreamSpecification spec : specification.getStreams().values()) {
      result = getVerifier(StreamSpecification.class).verify(appId, spec);
      if (!result.isSuccess()) {
        throw new RuntimeException(result.getMessage());
      }
      // if the stream existed verify the owner to be the same
      if (store.getStream(appId.getNamespaceId(), spec.getName()) != null) {
        verifyOwner(appId.getParent().stream(spec.getName()), specifiedOwnerPrincipal);
      }
    }
  }

  private void verifyOwner(NamespacedEntityId entityId, @Nullable KerberosPrincipalId specifiedOwnerPrincipal)
    throws DatasetManagementException, UnauthorizedException {
    try {
      SecurityUtil.verifyOwnerPrincipal(entityId,
                                        specifiedOwnerPrincipal == null ? null : specifiedOwnerPrincipal.getPrincipal(),
                                        ownerAdmin);
    } catch (IOException e) {
      throw new DatasetManagementException(e.getMessage(), e);
    }
  }

  protected void verifyPrograms(ApplicationId appId, ApplicationSpecification specification) {
    Iterable<ProgramSpecification> programSpecs = Iterables.concat(specification.getFlows().values(),
                                                                   specification.getMapReduce().values(),
                                                                   specification.getWorkflows().values());
    VerifyResult result;
    for (ProgramSpecification programSpec : programSpecs) {
      result = getVerifier(programSpec.getClass()).verify(appId, programSpec);
      if (!result.isSuccess()) {
        throw new RuntimeException(result.getMessage());
      }
    }

    for (Map.Entry<String, WorkflowSpecification> entry : specification.getWorkflows().entrySet()) {
      verifyWorkflowSpecifications(specification, entry.getValue());
    }

    for (Map.Entry<String, ScheduleSpecification> entry : specification.getSchedules().entrySet()) {
      ScheduleProgramInfo program = entry.getValue().getProgram();
      switch (program.getProgramType()) {
        case WORKFLOW:
          if (!specification.getWorkflows().containsKey(program.getProgramName())) {
            throw new RuntimeException(String.format("Workflow '%s' is not configured with the Application.",
                                                     program.getProgramName()));
          }
          break;
        default:
          throw new RuntimeException(String.format("Program '%s' with Program Type '%s' cannot be scheduled.",
                                                   program.getProgramName(), program.getProgramType()));
      }

      // TODO StreamSizeSchedules should be resilient to stream inexistence [CDAP-1446]
      Schedule schedule = entry.getValue().getSchedule();
      if (schedule instanceof StreamSizeSchedule) {
        StreamSizeSchedule streamSizeSchedule = (StreamSizeSchedule) schedule;
        String streamName = streamSizeSchedule.getStreamName();
        if (!specification.getStreams().containsKey(streamName) &&
          store.getStream(appId.getParent(), streamName) == null) {
          throw new RuntimeException(String.format("Schedule '%s' uses a Stream '%s' that does not exit",
                                                   streamSizeSchedule.getName(), streamName));
        }
      }
    }
  }

  private void verifyWorkflowSpecifications(ApplicationSpecification appSpec, WorkflowSpecification workflowSpec) {
    Set<String> existingNodeNames = new HashSet<>();
    verifyWorkflowNodeList(appSpec, workflowSpec, workflowSpec.getNodes(), existingNodeNames);
  }

  private void verifyWorkflowNode(ApplicationSpecification appSpec, WorkflowSpecification workflowSpec,
                                  WorkflowNode node, Set<String> existingNodeNames) {
    WorkflowNodeType nodeType = node.getType();
    // TODO CDAP-5640 Add check so that node id in the Workflow should not be same as name of the Workflow.
    if (node.getNodeId().equals(workflowSpec.getName())) {
      String msg = String.format("Node used in Workflow has same name as that of Workflow '%s'." +
                                   " This will conflict while getting the Workflow token details associated with" +
                                   " the node. Please use name for the node other than the name of the Workflow.",
                                 workflowSpec.getName());
      LOG.warn(msg);
    }
    switch (nodeType) {
      case ACTION:
        verifyWorkflowAction(appSpec, node);
        break;
      case FORK:
        verifyWorkflowFork(appSpec, workflowSpec, node, existingNodeNames);
        break;
      case CONDITION:
        verifyWorkflowCondition(appSpec, workflowSpec, node, existingNodeNames);
        break;
      default:
        break;
    }
  }

  private void verifyWorkflowFork(ApplicationSpecification appSpec, WorkflowSpecification workflowSpec,
                                  WorkflowNode node, Set<String> existingNodeNames) {
    WorkflowForkNode forkNode = (WorkflowForkNode) node;
    Preconditions.checkNotNull(forkNode.getBranches(), String.format("Fork is added in the Workflow '%s' without" +
                                                                       " any branches", workflowSpec.getName()));

    for (List<WorkflowNode> branch : forkNode.getBranches()) {
      verifyWorkflowNodeList(appSpec, workflowSpec, branch, existingNodeNames);
    }
  }

  private void verifyWorkflowCondition(ApplicationSpecification appSpec, WorkflowSpecification workflowSpec,
                                       WorkflowNode node, Set<String> existingNodeNames) {
    WorkflowConditionNode condition = (WorkflowConditionNode) node;
    verifyWorkflowNodeList(appSpec, workflowSpec, condition.getIfBranch(), existingNodeNames);
    verifyWorkflowNodeList(appSpec, workflowSpec, condition.getElseBranch(), existingNodeNames);
  }

  private void verifyWorkflowNodeList(ApplicationSpecification appSpec, WorkflowSpecification workflowSpec,
                                      List<WorkflowNode> nodeList, Set<String> existingNodeNames) {
    for (WorkflowNode n : nodeList) {
      if (existingNodeNames.contains(n.getNodeId())) {
        throw new RuntimeException(String.format("Node '%s' already exists in workflow '%s'.", n.getNodeId(),
                                                 workflowSpec.getName()));
      }
      existingNodeNames.add(n.getNodeId());
      verifyWorkflowNode(appSpec, workflowSpec, n, existingNodeNames);
    }
  }

  private void verifyWorkflowAction(ApplicationSpecification appSpec, WorkflowNode node) {
    WorkflowActionNode actionNode = (WorkflowActionNode) node;
    ScheduleProgramInfo program = actionNode.getProgram();
    switch (program.getProgramType()) {
      case MAPREDUCE:
        Preconditions.checkArgument(appSpec.getMapReduce().containsKey(program.getProgramName()),
                                    String.format("MapReduce program '%s' is not configured with the Application.",
                                                  program.getProgramName()));
        break;
      case SPARK:
        Preconditions.checkArgument(appSpec.getSpark().containsKey(program.getProgramName()),
                                    String.format("Spark program '%s' is not configured with the Application.",
                                                  program.getProgramName()));
        break;
      case CUSTOM_ACTION:
        // no-op
        break;
      default:
        throw new RuntimeException(String.format("Unknown Program '%s' in the Workflow.",
                                                 program.getProgramName()));
    }
  }


  @SuppressWarnings("unchecked")
  private <T> Verifier<T> getVerifier(Class<? extends T> clz) {
    if (verifiers.containsKey(clz)) {
      return (Verifier<T>) verifiers.get(clz);
    }

    if (ApplicationSpecification.class.isAssignableFrom(clz)) {
      verifiers.put(clz, new ApplicationVerification());
    } else if (StreamSpecification.class.isAssignableFrom(clz)) {
      verifiers.put(clz, new StreamVerification());
    } else if (FlowSpecification.class.isAssignableFrom(clz)) {
      verifiers.put(clz, new FlowVerification());
    } else if (ProgramSpecification.class.isAssignableFrom(clz)) {
      verifiers.put(clz, createProgramVerifier((Class<ProgramSpecification>) clz));
    } else if (DatasetCreationSpec.class.isAssignableFrom(clz)) {
      verifiers.put(clz, new DatasetCreationSpecVerifier());
    }

    return (Verifier<T>) verifiers.get(clz);
  }

  private <T extends ProgramSpecification> Verifier<T> createProgramVerifier(Class<T> clz) {
    return new ProgramVerification<>();
  }
}
