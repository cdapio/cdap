package com.continuuity.internal.app.verification;

import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.verification.AbstractVerifier;
import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.error.Err;

/**
 *
 */
public class ProcedureVerification extends AbstractVerifier implements Verifier<ProcedureSpecification> {

  @Override
  public VerifyResult verify(final ProcedureSpecification input) {
    // Checks if Procedure name is an ID
    if(! isId(input.getName())) {
      return VerifyResult.FAILURE(Err.NOT_AN_ID, "Procedure");
    }
    return VerifyResult.SUCCESS();
  }
}
