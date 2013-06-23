package com.continuuity.internal.app.verification;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.verification.AbstractVerifier;
import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.error.Err;

/**
 * This class is responsible for verifying the Application details of
 * the {@link ApplicationSpecification}.
 * <p/>
 * <p>
 * Following are the checks done for Application
 * <ul>
 * <li>Application name is an ID</li>
 * <li>Application contains at one of the following: Flow, Procedure, Batch/MR</li>
 * </ul>
 * </p>
 */
public class ApplicationVerification extends AbstractVerifier implements Verifier<ApplicationSpecification> {

  /**
   * Verifies {@link ApplicationSpecification} of the {@link com.continuuity.api.Application}
   * being provide.
   *
   * @param input to be verified
   * @return An instance of {@link VerifyResult} depending of status of verification.
   */
  @Override
  public VerifyResult verify(final ApplicationSpecification input) {
    // Check if Application name is an ID or no.
    if(!isId(input.getName())) {
      return VerifyResult.FAILURE(Err.NOT_AN_ID, "Application");
    }

    // Check if there is at least one of the following : Flow & Procedure or MapReduce for now.
    if(input.getProcedures().size() == 0 && input.getFlows().size() == 0 && input.getMapReduces().size() == 0) {
      return VerifyResult.FAILURE(Err.Application.ATLEAST_ONE_PROCESSOR, input.getName());
    }

    return VerifyResult.SUCCESS();
  }
}
