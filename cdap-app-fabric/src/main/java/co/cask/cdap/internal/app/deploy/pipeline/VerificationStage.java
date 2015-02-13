/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowFork;
import co.cask.cdap.api.workflow.WorkflowForkBranch;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowNodeType;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.verification.Verifier;
import co.cask.cdap.app.verification.VerifyResult;
import co.cask.cdap.data.dataset.DatasetCreationSpec;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.internal.app.runtime.adapter.AdapterTypeInfo;
import co.cask.cdap.internal.app.verification.ApplicationVerification;
import co.cask.cdap.internal.app.verification.DatasetCreationSpecVerifier;
import co.cask.cdap.internal.app.verification.FlowVerification;
import co.cask.cdap.internal.app.verification.ProgramVerification;
import co.cask.cdap.internal.app.verification.StreamVerification;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.util.List;
import java.util.Map;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for verifying
 * the specification and components of specification. Verification of each
 * component of specification is achieved by the {@link Verifier}
 * concrete implementations.
 */
public class VerificationStage extends AbstractStage<ApplicationDeployable> {

  private final Map<Class<?>, Verifier<?>> verifiers = Maps.newIdentityHashMap();
  private final DatasetFramework dsFramework;
  private final AdapterService adapterService;

  public VerificationStage(DatasetFramework dsFramework, AdapterService adapterService) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.dsFramework = dsFramework;
    this.adapterService = adapterService;
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
    Id.Application appId = input.getId();

    if (ApplicationDeployScope.USER.equals(input.getApplicationDeployScope())) {
      AdapterTypeInfo adapterTypeInfo = adapterService.getAdapterTypeInfo(appId.getId());
      if (adapterTypeInfo != null) {
        throw new RuntimeException
          (String.format("Cannot deploy Application %s. An AdapterType exists with a conflicting name.", appId));
      }
    }

    VerifyResult result = getVerifier(ApplicationSpecification.class).verify(appId, specification);
    if (!result.isSuccess()) {
      throw new RuntimeException(result.getMessage());
    }

    // NOTE: no special restrictions on dataset module names, etc

    for (DatasetCreationSpec dataSetCreateSpec : specification.getDatasets().values()) {
      result = getVerifier(DatasetCreationSpec.class).verify(appId, dataSetCreateSpec);
      if (!result.isSuccess()) {
        throw new RuntimeException(result.getMessage());
      }
      String dsName = dataSetCreateSpec.getInstanceName();
      DatasetSpecification existingSpec = dsFramework.getDatasetSpec(dsName);
      if (existingSpec != null && !existingSpec.getType().equals(dataSetCreateSpec.getTypeName())) {
          // New app trying to deploy an dataset with same instanceName but different Type than that of existing.
          throw new DataSetException
            (String.format("Cannot Deploy Dataset : %s with Type : %s : Dataset with different Type Already Exists",
                           dsName, dataSetCreateSpec.getTypeName()));
        }
    }

    for (StreamSpecification spec : specification.getStreams().values()) {
      result = getVerifier(StreamSpecification.class).verify(appId, spec);
      if (!result.isSuccess()) {
        throw new RuntimeException(result.getMessage());
      }
    }

    Iterable<ProgramSpecification> programSpecs = Iterables.concat(specification.getFlows().values(),
                                                                   specification.getMapReduce().values(),
                                                                   specification.getProcedures().values(),
                                                                   specification.getWorkflows().values());

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
    }

    // Emit the input to next stage.
    emit(input);
  }

  private void verifyWorkflowSpecifications(ApplicationSpecification appSpec, WorkflowSpecification workflowSpec) {
    List<WorkflowNode> nodes = workflowSpec.getNodes();
    for (WorkflowNode node : nodes) {
      verifyWorkflowNode(appSpec, workflowSpec, node);
    }
  }

  private void verifyWorkflowNode(ApplicationSpecification appSpec, WorkflowSpecification workflowSpec,
                                  WorkflowNode node) {
    WorkflowNodeType nodeType = node.getType();
    switch (nodeType) {
      case ACTION:
        verifyWorkflowAction(appSpec, node.getProgramInfo());
        break;
      case FORK:
        for (Map.Entry<String, WorkflowFork> entry : workflowSpec.getForks().entrySet()) {
          List<WorkflowForkBranch> forkBranches = entry.getValue().getBranches();
          if (forkBranches == null) {
            throw new RuntimeException(String.format("Fork is added in the Workflow '%s' without any branches",
                                                     workflowSpec.getName()));
          }

          for (WorkflowForkBranch branch : forkBranches) {
            for (WorkflowNode n : branch.getNodes()) {
              verifyWorkflowNode(appSpec, workflowSpec, n);
            }
          }
        }
        break;
      case CONDITION:
        // no-op
        break;
      default:
        break;
    }
  }

  private void verifyWorkflowAction(ApplicationSpecification appSpec, ScheduleProgramInfo program) {
    switch (program.getProgramType()) {
      case MAPREDUCE:
        if (!appSpec.getMapReduce().containsKey(program.getProgramName())) {
          throw new RuntimeException(String.format("MapReduce program '%s' is not configured with the Application.",
                                                   program.getProgramName()));
        }
        break;
      case SPARK:
        if (!appSpec.getSpark().containsKey(program.getProgramName())) {
          throw new RuntimeException(String.format("Spark program '%s' is not configured with the Application.",
                                                   program.getProgramName()));
        }
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
    return new ProgramVerification<T>();
  }
}
