package com.continuuity.examples.countoddandeven;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;

/**
 * CountOddAndEven application contains a flow {@code CountOddAndEvenFlow}.
 */
public class CountOddAndEven implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("CountOddAndEven")
      .setDescription("Example application that counts odd and even " +
          "random numbers")
      .noStream()
      .noDataSet()
      .withFlows()
        .add(new CountOddAndEvenFlow())
      .noProcedure()
      .build();
  }
}
