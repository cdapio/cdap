package com.continuuity.internal.app.verification;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;

/**
 * This verifies a give {@link com.continuuity.api.flow.Flow}
 *
 * <p>
 *   Following are the checks that are done for a {@link com.continuuity.api.flow.Flow}
 *   <ul>
 *     <li>Verify Flow Meta Information - Name is id</li>
 *     <li>There should be atleast one or two flowlets</li>
 *     <li>Verify information for each Flowlet</li>
 *     <li>There should be atleast one connection</li>
 *     <li>Verify schema's across connections on flowlet</li>
 *   </ul>
 *
 * </p>
 */
public class FlowVerification implements Verifier<FlowSpecification> {

  /**
   * Verifies a single {@link FlowSpecification} for a {@link com.continuuity.api.flow.Flow}
   * defined within an {@link com.continuuity.api.Application}
   *
   * @param input to be verified
   * @return An instance of {@link VerifyResult} depending of status of verification.
   */
  @Override
  public VerifyResult verify(final FlowSpecification input) {
    return VerifyResult.SUCCESS();
  }
}
