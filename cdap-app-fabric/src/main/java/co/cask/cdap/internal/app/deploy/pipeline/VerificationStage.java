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
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.verification.Verifier;
import co.cask.cdap.app.verification.VerifyResult;
import co.cask.cdap.data.dataset.DatasetCreationSpec;
import co.cask.cdap.data2.dataset2.DatasetFramework;
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

  public VerificationStage(DatasetFramework dsFramework) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.dsFramework = dsFramework;
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

    // TODO: add a check against system applications (adapters, for instance).
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
      List<ScheduleProgramInfo> programs = entry.getValue().getActions();
      for (ScheduleProgramInfo program : programs) {
        switch (program.getProgramType()) {
          case MAPREDUCE:
            if (!specification.getMapReduce().containsKey(program.getProgramName())) {
              throw new RuntimeException(String.format("MapReduce program '%s' is not configured with the Application.",
                                                       program.getProgramName()));
            }
            break;
          case SPARK:
            if (!specification.getSpark().containsKey(program.getProgramName())) {
              throw new RuntimeException(String.format("Spark program '%s' is not configured with the Application.",
                                                       program.getProgramName()));
            }
            break;
          case CUSTOM_ACTION:
            // no-op
            break;
          default:
            throw new RuntimeException(String.format("Unknown Program '%s', Program Type '%s' in the Workflow.",
                                                     program.getProgramName()));
        }
      }
    }

    // Emit the input to next stage.
    emit(input);
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
