package com.continuuity.internal.app.verification;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.verification.Stage;

/**
 *
 */
public class ApplicationVerification implements Stage<ApplicationSpecification, Boolean> {

  @Override
  public Boolean process(final ApplicationSpecification data) {
    return false;
  }
}
