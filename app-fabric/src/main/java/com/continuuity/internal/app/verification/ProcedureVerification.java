package com.continuuity.internal.app.verification;

import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.verification.AbstractVerifier;
import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.error.Err;

/**
 * This class verifies a {@link ProcedureVerification}.
 * <p>
 * Following are the checks that are done for Procedure.
 * <ul>
 * <li>Check if the procedure name is an id or not</li>
 * </ul>
 * </p>
 */
public class ProcedureVerification extends AbstractVerifier implements Verifier<ProcedureSpecification> {

  /**
   * Verifies a given {@link ProcedureSpecification}
   *
   * @param input {@link ProcedureSpecification} to be verified
   * @return An instance of {@link VerifyResult} depending of status of verification.
   */
  @Override
  public VerifyResult verify(final ProcedureSpecification input) {
    // Checks if Procedure name is an ID
    if (!isId(input.getName())) {
      return VerifyResult.failure(Err.NOT_AN_ID, "Procedure");
    }
    return VerifyResult.success();
  }
}
