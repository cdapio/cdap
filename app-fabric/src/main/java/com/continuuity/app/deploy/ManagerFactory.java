/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.deploy;

import com.continuuity.internal.app.deploy.ProgramTerminator;

/**
 *
 */
public interface ManagerFactory {
  <U, V> Manager<U, V> create(ProgramTerminator handler);
}
