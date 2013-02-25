/*
 * TwitterScanner - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.examples.twitter;

import java.util.ArrayList;
import java.util.List;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

public class TwitterProcessor extends AbstractFlowlet {

  @UseDataSet(TwitterScanner.topHashTags)
  private SortedCounterTable topHashTags;

  @UseDataSet(TwitterScanner.topUsers)
  private SortedCounterTable topUsers;

  @UseDataSet(TwitterScanner.topUsers)
  private CounterTable wordCounts;

  @UseDataSet(TwitterScanner.hashTagWordAssocs)
  private CounterTable hashTagWordAssocs;

  public void process(Tweet tweet) throws OperationException {
    String [] words = tweet.getText().split("\\s+");

    List<String> goodWords = new ArrayList<String>(words.length);
    for (String word : words) {
      if (word.length() < 3) continue;
      if (!word.equals(new String(word.getBytes()))) continue;
      goodWords.add(word.toLowerCase());
    }
    // Split tweet into individual words
    for (String word : goodWords) {
      if (word.startsWith("#")) {
        // Track top hash tags
        topHashTags.increment(
            TwitterScanner.HASHTAG_SET, Bytes.toBytes(word), 1L);
        // For every hash tag, track word associations
        for (String corWord : goodWords) {
          if (corWord.startsWith("#")) continue;
          hashTagWordAssocs.incrementCounterSet(word, corWord, 1L);
        }
      } else {
        // Track word counts
        wordCounts.incrementCounterSet(
            TwitterScanner.WORD_SET, Bytes.toBytes(word), 1L);
      }
    }

    // Track top users
    topUsers.increment(
      TwitterScanner.USER_SET, Bytes.toBytes(tweet.getUser()), 1L);
  }
}
