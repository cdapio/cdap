/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.deploy;

import com.continuuity.common.conf.CConfiguration;

/**
 *
 */
public interface ManagerFactory {
  Manager<?, ?> create(CConfiguration configuration);
}
