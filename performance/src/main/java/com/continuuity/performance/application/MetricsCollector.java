/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.application;

import com.continuuity.common.conf.CConfiguration;

/**
 *
 */
public interface MetricsCollector extends Runnable {

  void configure(CConfiguration config);

  void stop();
}
