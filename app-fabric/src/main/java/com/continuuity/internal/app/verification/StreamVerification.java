package com.continuuity.internal.app.verification;

import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.app.verification.AbstractVerifier;

/**
 *
 */
public class StreamVerification extends AbstractVerifier<StreamSpecification> {

  @Override
  protected String getName(StreamSpecification input) {
    return input.getName();
  }
}
