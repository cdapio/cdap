package com.continuuity.examples.twitter;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.common.Bytes;

/**
 *
 */
public class TwitterScanner implements Application {

  // DataSets
  public static final String topUsers = "topUsers";
  public static final String topHashTags = "topHashTags";
  public static final String wordCounts = "wordCounts";
  public static final String hashTagWordAssocs = "hashTagWordAssocs";

  // DataSet Constants
  public static final byte [] HASHTAG_SET = Bytes.toBytes("h");
  public static final byte [] USER_SET = Bytes.toBytes("u");
  public static final byte [] WORD_SET = Bytes.toBytes("w");
  
  // Twitter Constants
  // TODO: Remove this and change to runtime parameters
  public static final String [] USERNAMES = new String [] {
      "consumerintelli", "continuuity", "bigflowdev"
  };
  public static final String PASSWORD = "Wh00pa$$123!";

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("TwitterScanner")
      .setDescription("Example Twitter application")
      .noStream()
      .withDataSets()
        .add(new SortedCounterTable(topUsers,
            new SortedCounterTable.SortedCounterConfig()))
        .add(new SortedCounterTable(topHashTags,
            new SortedCounterTable.SortedCounterConfig()))
        .add(new CounterTable(wordCounts))
        .add(new CounterTable(hashTagWordAssocs))
      .withFlows()
        .add(new TwitterFlow())
      .withProcedures()
        .add(new TwitterProcedure())
      .build();
  }
}