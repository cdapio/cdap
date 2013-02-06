/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 *
 * The original Apache 2.0 licensed code was changed by Continuuity Inc.
 */

package com.continuuity.common.conf;

public interface Configurable {

  /** Set the configuration to be used by this object. */
  void setConf(Configuration conf);

  /** Return the configuration used by this object. */
  Configuration getConf();
}
