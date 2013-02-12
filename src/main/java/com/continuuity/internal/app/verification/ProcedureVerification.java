package com.continuuity.internal.app.verification;

import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;

/**
 *
 */
public class ProcedureVerification implements Verifier<ProcedureVerification> {

  @Override
  public VerifyResult verify(final ProcedureVerification input) {
    return VerifyResult.SUCCESS();
  }
}
