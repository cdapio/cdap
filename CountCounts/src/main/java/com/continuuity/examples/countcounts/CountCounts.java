package com.continuuity.examples.countcounts;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.stream.Stream;

/**
 * CountCountsDemo application contains a flow {@code CountCounts} and is attached
 * to a stream named "text"
 */
public class CountCounts implements Application {

  public static final String tableName = "countCounterTable";

  public static void main(String[] args) {
    // Main method should be defined for Application to get deployed with Eclipse IDE plugin. DO NOT REMOVE IT
  }

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("CountCounts")
      .setDescription("Application for counting counts of words")
      .withStreams()
        .add(new Stream("text"))
      .withDataSets()
        .add(new CountCounterTable(tableName))
      .withFlows()
        .add(new CountCountsFlow())
      .withProcedures()
        .add(new CountCountsProcedure())
      .build();
  }
}