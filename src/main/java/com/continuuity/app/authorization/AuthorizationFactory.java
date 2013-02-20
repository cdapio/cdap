/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.authorization;

import com.continuuity.common.conf.CConfiguration;

/**
 * Factory for handling authorization service.
 */
public interface AuthorizationFactory {
  /**
   * Creates an instance of {@link AuthorizationHandler} for authorizing requests
   * being processed by any service.
   *
   * @param configuration An instance of {@link CConfiguration} to configure.
   * @return An instance of {@link AuthorizationHandler}
   */
  AuthorizationHandler create(CConfiguration configuration);
}
