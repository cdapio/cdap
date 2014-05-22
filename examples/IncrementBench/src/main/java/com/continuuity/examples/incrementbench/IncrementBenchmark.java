package com.continuuity.examples.incrementbench;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.table.Table;

/**
 *
 */
public class IncrementBenchmark implements Application {
  public static final String TABLE = "incrementTable";
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("IncrementBenchmark")
      .setDescription("Simple benchmark that writes increments to a Table")
      .noStream()
      .withDataSets()
      .add(new Table(TABLE))
      .withFlows()
      .add(new IncrementFlow())
      .noProcedure()
      .noMapReduce()
      .noWorkflow()
      .build();
  }
}
