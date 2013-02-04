/*
 * TwitterScanner - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.examples.twitter;

import com.continuuity.api.data.Closure;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;

import java.util.ArrayList;
import java.util.List;

public class TwitterProcessor extends ComputeFlowlet {

  @Override
  public void configure(FlowletSpecifier specifier) {
    specifier.getDefaultFlowletInput().setSchema(
        TwitterFlow.TWEET_SCHEMA);
    specifier.getDefaultFlowletOutput().setSchema(
        TwitterFlow.POST_PROCESS_SCHEMA);
  }

  private SortedCounterTable topHashTags;
  private SortedCounterTable topUsers;

  private CounterTable wordCounts;
  private CounterTable hashTagWordAssocs;

  @Override
  public void initialize() {
    this.topHashTags = getFlowletContext().getDataSet(TwitterFlow.topHashTags);
    this.topUsers = getFlowletContext().getDataSet(TwitterFlow.topUsers);

    this.wordCounts = getFlowletContext().getDataSet(TwitterFlow.wordCounts);
    this.hashTagWordAssocs = getFlowletContext().getDataSet(TwitterFlow.hashTagWordAssocs);
  }

  @Override
  public void process(Tuple tuple, TupleContext context,
      OutputCollector collector) {

    Tweet tweet = tuple.get("tweet");
    
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
        Closure closure = topHashTags.generatePrimaryCounterIncrement(
            TwitterFlow.HASHTAG_SET, Bytes.toBytes(word), 1L);
        Tuple outTuple = new TupleBuilder()
            .set("name", word)
            .set("value", closure)
            .create();
        collector.add(outTuple);
        // And for every hash tag, track word associations
        for (String corWord : goodWords) {
          if (corWord.startsWith("#")) continue;
//          System.out.println("Correlating (" + word + ") with (" + corWord + ")");
          try {
            hashTagWordAssocs.incrementCounterSet(word, corWord, 1L);
          } catch (OperationException e) {
            // This doesn't seem to work any way I try :(
            e.printStackTrace();
          }
        }
      } else {
        // Track word counts
        wordCounts.incrementCounterSet(TwitterFlow.WORD_SET, Bytes.toBytes(word), 1L);
      }
    }

    // Track top users
    Closure closure = topUsers.generatePrimaryCounterIncrement(
        TwitterFlow.USER_SET, Bytes.toBytes(tweet.getUser()), 1L);
    Tuple outTuple = new TupleBuilder()
        .set("name", tweet.getUser())
        .set("value", closure)
        .create();
    
    collector.add(outTuple);
  }

}
