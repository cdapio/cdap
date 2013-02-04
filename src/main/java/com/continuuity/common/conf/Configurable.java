/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.common.conf;

public interface Configurable {

  /** Set the configuration to be used by this object. */
  void setConf(Configuration conf);

  /** Return the configuration used by this object. */
  Configuration getConf();
}
