/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.store;

import com.continuuity.common.conf.CConfiguration;

/**
 *
 */
public interface StoreFactory {
  Store create(CConfiguration configuration);
}
