package com.continuuity.internal.app.verification;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.app.verification.Stage;

/**
 *
 */
public class FlowVerification implements Stage<FlowSpecification, Boolean> {

  @Override
  public Boolean process(final FlowSpecification data) {
    return false;
  }
}
