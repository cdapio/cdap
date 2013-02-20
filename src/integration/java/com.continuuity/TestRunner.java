/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Runner for running AppFabricIntegration Suite.
 */
public class TestRunner {
  private static Logger LOG = LoggerFactory.getLogger(TestRunner.class);
  public static void main(String[] args) {
    Result result = JUnitCore.runClasses(AppFabricIntegrationSuite.class);
    for(Failure failure : result.getFailures()) {
      LOG.error(failure.toString());
    }
    LOG.info(result.wasSuccessful() ? "SUCCESS" : "FAILURE");
  }
}
