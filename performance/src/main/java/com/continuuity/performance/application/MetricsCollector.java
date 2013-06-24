package com.continuuity.performance.application;

import com.continuuity.common.conf.CConfiguration;

/**
 *
 */
public interface MetricsCollector extends Runnable {

  void configure(CConfiguration config);

  void stop();
}
