package com.continuuity.internal.app.verification;

import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.app.verification.AbstractVerifier;
import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.error.Err;

/**
 *
 */
public class StreamVerification extends AbstractVerifier implements Verifier<StreamSpecification> {

  @Override
  public VerifyResult verify(final StreamSpecification input) {
    // Checks if DataSet name is an ID
    if (!isId(input.getName())) {
      return VerifyResult.failure(Err.NOT_AN_ID, "Stream");
    }
    return VerifyResult.success();
  }
}
