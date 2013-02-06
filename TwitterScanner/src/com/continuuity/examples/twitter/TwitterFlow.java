/*
 * twitterScanner - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.examples.twitter;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;

public class TwitterFlow implements Flow {

  public static final String topUsers = "topUsers";
  public static final String topHashTags = "topHashTags";
  public static final String wordCounts = "wordCounts";
  public static final String hashTagWordAssocs = "hashTagWordAssocs";

  public static final TupleSchema TWEET_SCHEMA =
      new TupleSchemaBuilder().add("tweet", Tweet.class).create();

  public static final TupleSchema POST_PROCESS_SCHEMA =
      new TupleSchemaBuilder()
          .add("name", String.class)
          .add("value", Long.class)
          .create();

  public static final String [] USERNAMES = new String [] {
      "consumerintelli", "continuuity", "bigflowdev"
  };
  
  public static final String PASSWORD = "Wh00pa$$123!";

  public static final byte [] HASHTAG_SET = Bytes.toBytes("h");
  public static final byte [] USER_SET = Bytes.toBytes("u");
  public static final byte [] WORD_SET = Bytes.toBytes("w");
  
  @Override
  public void configure(FlowSpecifier flowSpecifier) {

    // Set up some meta data
    flowSpecifier.name("TwitterScanner");
    flowSpecifier.email("dev@continuuity.com");
    flowSpecifier.application("Twitter Demo");

    // add the sorted counters data set
    flowSpecifier.dataset(topHashTags);
    flowSpecifier.dataset(topUsers);
    flowSpecifier.dataset(wordCounts);
    flowSpecifier.dataset(hashTagWordAssocs);

    // Now wire up the Flow for real
    flowSpecifier.flowlet("StreamReader", TwitterGenerator.class, 1);
    flowSpecifier.flowlet("Processor", TwitterProcessor.class, 1);
    flowSpecifier.flowlet("WordIndexer", TwitterWordIndexer.class, 1);
    flowSpecifier.flowlet("HashTagIndexer", TwitterHashTagIndexer.class, 1);

    // Connect to the next Flowlet
    flowSpecifier.connection("StreamReader", "Processor");
    flowSpecifier.connection("Processor", "WordIndexer");
    flowSpecifier.connection("Processor", "HashTagIndexer");

  }
}
