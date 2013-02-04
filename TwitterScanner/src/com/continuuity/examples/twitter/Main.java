package com.continuuity.examples.twitter;

import com.continuuity.api.flow.Application;
import com.continuuity.api.flow.ApplicationSpecification;

/**
 *
 */
public class Main implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.builder()
      .setApplicationName("TwitterApp")
      .addFlow(TwitterFlow.class)
      .addQuery(TwitterQuery.class)
      .addDataSet(new SortedCounterTable(TwitterFlow.topUsers, new SortedCounterTable.SortedCounterConfig()))
      .addDataSet(new SortedCounterTable(TwitterFlow.topHashTags, new SortedCounterTable.SortedCounterConfig()))
      .addDataSet(new CounterTable(TwitterFlow.wordCounts))
      .addDataSet(new CounterTable(TwitterFlow.hashTagWordAssocs))
      .create();
  }
}
