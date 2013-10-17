package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;

/**
 * App with Flow and MapReduce. used for testing MDS.
 */
public class FlowMapReduceApp implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("App")
      .setDescription("Application which has everything")
      .withStreams()
      .add(new Stream("stream"))
      .withDataSets()
       .add(new KeyValueTable("kvt"))
      .withFlows()
        .add(new AllProgramsApp.NoOpFlow())
      .noProcedure()
      .withMapReduce()
        .add(new AllProgramsApp.NoOpMR())
      .noWorkflow()
      .build();
  }
}
