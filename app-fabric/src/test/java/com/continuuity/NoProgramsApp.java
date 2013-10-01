package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;

/**
 *
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
      .noBatch()
      .noWorkflow()
      .build();
  }
}
