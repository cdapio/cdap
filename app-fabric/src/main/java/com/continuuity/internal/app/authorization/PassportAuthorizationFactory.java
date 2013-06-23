/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.authorization;

import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.authorization.AuthorizationHandler;
import com.continuuity.common.conf.CConfiguration;

/**
 *
 */
public class PassportAuthorizationFactory implements AuthorizationFactory {

  /**
   * Creates an instance of {@link com.continuuity.app.authorization.AuthorizationHandler} for authorizing requests
   * being processed by any service.
   *
   * @param configuration An instance of {@link com.continuuity.common.conf.CConfiguration} to configure.
   * @return An instance of {@link com.continuuity.app.authorization.AuthorizationHandler}
   */
  @Override
  public AuthorizationHandler create(CConfiguration configuration) {
    return new PassportAuthorizationHandler(configuration);
  }
}
