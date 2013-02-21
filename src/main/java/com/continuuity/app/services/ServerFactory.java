/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.services;

import com.continuuity.common.conf.CConfiguration;
import com.google.common.util.concurrent.Service;

/**
 *
 */
public interface ServerFactory {
  Service create(CConfiguration configuration);
}
