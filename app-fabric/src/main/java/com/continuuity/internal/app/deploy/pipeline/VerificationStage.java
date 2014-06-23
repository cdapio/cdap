/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.api.ProgramSpecification;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.data.dataset.DatasetCreationSpec;
import com.continuuity.internal.app.verification.ApplicationVerification;
import com.continuuity.internal.app.verification.DataSetVerification;
import com.continuuity.internal.app.verification.DatasetCreationSpecVerifier;
import com.continuuity.internal.app.verification.FlowVerification;
import com.continuuity.internal.app.verification.ProgramVerification;
import com.continuuity.internal.app.verification.StreamVerification;
import com.continuuity.pipeline.AbstractStage;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.util.Map;

/**
 * This {@link com.continuuity.pipeline.Stage} is responsible for verifying
 * the specification and components of specification. Verification of each
 * component of specification is achieved by the {@link Verifier}
 * concrete implementations.
 */
public class VerificationStage extends AbstractStage<ApplicationSpecLocation> {

  private final Map<Class<?>, Verifier<?>> verifiers = Maps.newIdentityHashMap();

  public VerificationStage() {
    super(TypeToken.of(ApplicationSpecLocation.class));
  }

  /**
   * Receives an input containing application specification and location
   * and verifies both.
   *
   * @param input An instance of {@link ApplicationSpecLocation}
   */
  @Override
  public void process(ApplicationSpecLocation input) throws Exception {
    Preconditions.checkNotNull(input);

    ApplicationSpecification specification = input.getSpecification();
    Id.Application appId = input.getApplicationId();

    VerifyResult result = getVerifier(ApplicationSpecification.class).verify(appId, specification);
    if (!result.isSuccess()) {
      throw new RuntimeException(result.getMessage());
    }

    for (DataSetSpecification spec : specification.getDataSets().values()) {
      result = getVerifier(DataSetSpecification.class).verify(appId, spec);
      if (!result.isSuccess()) {
        throw new RuntimeException(result.getMessage());
      }
    }

    // NOTE: no special restrictions on dataset module names, etc

    for (DatasetCreationSpec dataSetCreateSpec : specification.getDatasets().values()) {
      result = getVerifier(DatasetCreationSpec.class).verify(appId, dataSetCreateSpec);
      if (!result.isSuccess()) {
        throw new RuntimeException(result.getMessage());
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
    } else if (DataSetSpecification.class.isAssignableFrom(clz)) {
      verifiers.put(clz, new DataSetVerification());
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
