package com.continuuity.internal.app.verification;

import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;

/**
 *
 */
public class StreamVerification implements Verifier<StreamSpecification> {

  @Override
  public VerifyResult verify(final StreamSpecification input) {
    // Stream
    return VerifyResult.SUCCESS();
  }
}
