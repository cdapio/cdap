/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.internal.app.verification.ApplicationVerification;
import com.continuuity.internal.app.verification.DataSetVerification;
import com.continuuity.internal.app.verification.FlowVerification;
import com.continuuity.internal.app.verification.ProcedureVerification;
import com.continuuity.internal.app.verification.StreamVerification;
import com.continuuity.pipeline.AbstractStage;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link com.continuuity.pipeline.Stage} is responsible for verifying
 * the specification and components of specification. Verification of each
 * component of specification is achived by the {@link com.continuuity.app.verification.Verifier}
 * concrete implementations.
 */
public class VerificationStage extends AbstractStage<ApplicationSpecLocation> {
  private static Logger LOG = LoggerFactory.getLogger(VerificationStage.class);
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

    {
      ApplicationVerification verifier = new ApplicationVerification();
      VerifyResult result = verifier.verify(specification);
      if(result.getStatus() != VerifyResult.Status.SUCCESS) {
        throw new RuntimeException(result.getMessage());
      }
    }

    {
      DataSetVerification verifier = new DataSetVerification();
      for(DataSetSpecification spec : specification.getDataSets().values()) {
        VerifyResult result = verifier.verify(spec);
        if(result.getStatus() != VerifyResult.Status.SUCCESS) {
          throw new RuntimeException(result.getMessage());
        }
      }
    }

    {
      StreamVerification verifier = new StreamVerification();
      for(StreamSpecification spec : specification.getStreams().values()) {
        VerifyResult result = verifier.verify(spec);
        if(result.getStatus() != VerifyResult.Status.SUCCESS) {
          throw new RuntimeException(result.getMessage());
        }
      }
    }

    {
      FlowVerification verifier = new FlowVerification();
      for(FlowSpecification spec : specification.getFlows().values()) {
        VerifyResult result = verifier.verify(spec);
        if(result.getStatus() != VerifyResult.Status.SUCCESS) {
          throw new RuntimeException(result.getMessage());
        }
      }
    }

    {
      ProcedureVerification verifier = new ProcedureVerification();
      for(ProcedureSpecification spec : specification.getProcedures().values()) {
        VerifyResult result = verifier.verify(spec);
        if(result.getStatus() != VerifyResult.Status.SUCCESS) {
          throw new RuntimeException(result.getMessage());
        }
      }
    }

    // Emit the input to next stage.
    emit(input);
  }
}
