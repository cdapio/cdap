package com.continuuity.test.app;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.procedure.AbstractProcedure;

/**
 * Simple app with table dataset.
 */
public class AppWithTable implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("AppWithTable")
      .setDescription("Simple app with table dataset")
      .noStream()
      .withDataSets().add(new Table("my_table"))
      .noFlow()
      .withProcedures().add(new AbstractProcedure("fooProcedure") { })
      .noMapReduce()
      .noWorkflow()
      .build();
  }
}
