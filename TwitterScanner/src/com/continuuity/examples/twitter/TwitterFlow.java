/*
 * twitterScanner - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.examples.twitter;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

public class TwitterFlow implements Flow {

  public static final String topUsers = "topUsers";
  public static final String topHashTags = "topHashTags";
  public static final String wordCounts = "wordCounts";
  public static final String hashTagWordAssocs = "hashTagWordAssocs";

  public static final String [] USERNAMES = new String [] {
      "consumerintelli", "continuuity", "bigflowdev"
  };
  
  public static final String PASSWORD = "Wh00pa$$123!";

  public static final byte [] HASHTAG_SET = Bytes.toBytes("h");
  public static final byte [] USER_SET = Bytes.toBytes("u");
  public static final byte [] WORD_SET = Bytes.toBytes("w");

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("TwitterScanner")
      .setDescription("Twitter Demo")
      .withFlowlets()
        .add("StreamReader", new TwitterGenerator(), 1)
        .add("Processors", new TwitterProcessor(), 1)
        .add("WordIndexer", new TwitterWordIndexer(), 1)
        .add("HashTagIndexer", new TwitterHashTagIndexer(), 1)
      .connect()
        .from("StreamReader").to("Processor")
        .from("Processor").to("WordIndexer")
        .from("Processor").to("HashTagIndexer")
      .build();
  }
}
