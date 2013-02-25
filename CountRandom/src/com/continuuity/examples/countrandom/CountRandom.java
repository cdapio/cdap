package com.continuuity.examples.countrandom;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;

/**
 * CountRandomDemo application contains a flow {@code CountRandom}.
 */
public class CountRandom implements Application {

  public static final String tableName = "randomTable";

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("CountRandom")
      .setDescription("Example random count application")
      .noStream()
      .withDataSets()
        .add(new KeyValueTable(tableName))
      .withFlows()
        .add(new CountRandomFlow())
      .noProcedure()
      .build();
  }
}
