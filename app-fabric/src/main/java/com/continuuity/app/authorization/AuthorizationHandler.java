/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.authorization;

import com.continuuity.app.Id;

/**
 *
 */
public interface AuthorizationHandler {
  boolean authroize(Id.Account account);
}
