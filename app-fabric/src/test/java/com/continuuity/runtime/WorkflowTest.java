/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.runtime;

import com.continuuity.WorkflowApp;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.test.internal.TestHelper;
import org.junit.Test;

/**
 *
 */
public class WorkflowTest {

  @Test
  public void testWorkflow() throws Exception {
    final ApplicationWithPrograms app = TestHelper.deployApplicationWithManager(WorkflowApp.class);
    ProgramRunnerFactory runnerFactory = TestHelper.getInjector().getInstance(ProgramRunnerFactory.class);

    runnerFactory.create(ProgramRunnerFactory.Type.WORKFLOW);

  }
}
