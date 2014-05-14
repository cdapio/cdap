package com.continuuity.gateway.router;

import com.continuuity.security.auth.TokenState;
import com.continuuity.security.auth.TokenValidator;
import com.google.common.util.concurrent.AbstractService;

/**
 * Simple {@link com.continuuity.security.auth.TokenValidator} implementation for test cases, which always
 * returns {@link com.continuuity.security.auth.TokenState#VALID} for all tokens.
 */
public class SuccessTokenValidator extends AbstractService implements TokenValidator {
  @Override
  protected void doStart() {
    notifyStarted();
  }

  @Override
  protected void doStop() {
    notifyStopped();
  }

  @Override
  public TokenState validate(String token) {
    return TokenState.VALID;
  }
}
