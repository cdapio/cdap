package com.continuuity.examples.countoddandeven;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;

/**
 * CountOddAndEven application contains a flow {@code CountOddAndEvenFlow}.
 */
public class CountOddAndEven implements Application {
  public static void main(String[] args) {
    // Main method should be defined for Application to get deployed with Eclipse IDE plugin. DO NOT REMOVE IT
  }

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
