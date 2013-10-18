/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;

/**
 * This application has all sorts of
 */
public class DumbProgrammerApp implements Application {

  /**
   * Configures the {@link com.continuuity.api.Application} by
   * returning an {@link com.continuuity.api.ApplicationSpecification}
   *
   * @return An instance of {@code ApplicationSpecification}.
   */
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("DumbProgrammerApp$$$%")
      .setDescription("Told the name should be an Id")
      .noStream()
      .noDataSet()
      .noFlow()
      .noProcedure()
      .noMapReduce()
      .noWorkflow()
      .build();
  }
}
