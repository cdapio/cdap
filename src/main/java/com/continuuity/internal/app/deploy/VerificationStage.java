/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.verification.ApplicationVerification;
import com.continuuity.internal.app.verification.DataSetVerification;
import com.continuuity.internal.app.verification.FlowVerification;
import com.continuuity.internal.app.verification.ProcedureVerification;
import com.continuuity.internal.app.verification.StreamVerification;
import com.continuuity.pipeline.AbstractStage;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import java.util.List;

/**
 *
 */
public class VerificationStage extends AbstractStage<ApplicationSpecification> {

  protected VerificationStage() {
    super(TypeToken.of(ApplicationSpecification.class));
  }

  /**
   * Abstract process that does a safe cast to the type.
   *
   * @param o Object to be processed which is of type T
   */
  @Override
  public void process(ApplicationSpecification specification) throws Exception {
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
  }
}
