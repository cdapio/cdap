package com.continuuity.examples.twitter;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;

/**
 *
 */
public class Main implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("TwitterApp")
      .setDescription("")
      .noStream()
      .withDataSets()
        .add(new SortedCounterTable(TwitterFlow.topUsers, new SortedCounterTable.SortedCounterConfig()))
        .add(new SortedCounterTable(TwitterFlow.topHashTags, new SortedCounterTable.SortedCounterConfig()))
        .add(new CounterTable(TwitterFlow.wordCounts))
        .add(new CounterTable(TwitterFlow.hashTagWordAssocs))
      .withFlows()
        .add(new TwitterFlow())
      .withProcedures()
        .add(new TwitterQuery())
      .build();
  }
}