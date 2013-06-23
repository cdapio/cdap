/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.authorization;

import com.continuuity.app.Id;
import com.continuuity.app.authorization.AuthorizationHandler;
import com.continuuity.common.conf.CConfiguration;

/**
 *
 */
public class PassportAuthorizationHandler implements AuthorizationHandler {
  private final CConfiguration configuration;

  public PassportAuthorizationHandler(CConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public boolean authroize(Id.Account account) {
    return false;
  }
}
