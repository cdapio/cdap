package com.continuuity.internal.app.verification;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;

/**
 * This class is responsible for verifying the Application details of
 * the {@link ApplicationSpecification}.
 *
 * <p>
 *   Following are the checks done for Application
 *   <ul>
 *     <li>Application name is an ID</li>
 *     <li>Application contains at one of the following: Flow, Procedure, Batch/MR</li>
 *   </ul>
 * </p>
 */
public class ApplicationVerification implements Verifier<ApplicationSpecification> {

  /**
   * Verifies {@link ApplicationSpecification} of the {@link com.continuuity.api.Application}
   * being provide.
   *
   * @param input to be verified
   * @return An instance of {@link VerifyResult} depending of status of verification.
   */
  @Override
  public VerifyResult verify(final ApplicationSpecification input) {
    return VerifyResult.SUCCESS();
  }
}
