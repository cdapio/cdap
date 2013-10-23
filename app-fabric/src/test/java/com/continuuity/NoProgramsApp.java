package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;

/**
 * App with no programs. Used to test deleted program specifications.
 */
public class NoProgramsApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("App")
      .setDescription("Application which has nothing")
      .noStream()
      .noDataSet()
      .noFlow()
      .noProcedure()
      .noMapReduce()
      .noWorkflow()
      .build();
  }
}
