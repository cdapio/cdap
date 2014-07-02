package com.continuuity.internal.app.verification;

import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.verification.AbstractVerifier;
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
public class ApplicationVerification extends AbstractVerifier<ApplicationSpecification> {

  /**
   * Verifies {@link ApplicationSpecification} of the {@link com.continuuity.api.Application}
   * being provide.
   *
   * @param input to be verified
   * @return An instance of {@link VerifyResult} depending of status of verification.
   */
  @Override
  public VerifyResult verify(Id.Application appId, final ApplicationSpecification input) {
    VerifyResult verifyResult = super.verify(appId, input);

    if (!verifyResult.isSuccess()) {
      return verifyResult;
    }

    // Check if there is at least one of the following : Flow & Procedure or MapReduce or Workflow for now.
    // TODO (terence): Logic here is really not good. Need to refactor.
    if (input.getProcedures().isEmpty()
        && input.getFlows().isEmpty()
        && input.getMapReduce().isEmpty()
        && input.getWorkflows().isEmpty()
        && input.getServices().isEmpty()) {
      return VerifyResult.failure(Err.Application.ATLEAST_ONE_PROCESSOR, input.getName());
    }

    return VerifyResult.success();
  }

  @Override
  protected String getName(ApplicationSpecification input) {
    return input.getName();
  }
}
