/*
 * TwitterScanner - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.examples.twitter;

import java.util.ArrayList;
import java.util.List;

import com.continuuity.api.data.Increment;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.lib.CounterTable;
import com.continuuity.api.data.lib.SortedCounterTable;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;

public class TwitterProcessor extends ComputeFlowlet {

  @Override
  public void configure(FlowletSpecifier specifier) {
    specifier.getDefaultFlowletInput().setSchema(
        TwitterFlow.TWEET_SCHEMA);
    specifier.getDefaultFlowletOutput().setSchema(
        TwitterFlow.POST_PROCESS_SCHEMA);
  }

  private CounterTable wordCounts;

  private SortedCounterTable topHashTags;

  private SortedCounterTable topUsers;

  private CounterTable hashTagWordAssocs;

  @Override
  public void initialize() {
    this.wordCounts = (CounterTable)
        getFlowletContext().getDataSetRegistry().registerDataSet(
            new CounterTable("wordCounts"));
    this.topHashTags = (SortedCounterTable)
        getFlowletContext().getDataSetRegistry().registerDataSet(
            new SortedCounterTable("topHashTags",
            new SortedCounterTable.SortedCounterConfig()));
    this.topUsers = (SortedCounterTable)
        getFlowletContext().getDataSetRegistry().registerDataSet(
            new SortedCounterTable("topUsers",
            new SortedCounterTable.SortedCounterConfig()));
    this.hashTagWordAssocs = (CounterTable)
        getFlowletContext().getDataSetRegistry().registerDataSet(
            new CounterTable("hashTagWordAssocs"));
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
        Increment increment = topHashTags.generatePrimaryCounterIncrement(
            TwitterFlow.HASHTAG_SET, Bytes.toBytes(word), 1L);
        Tuple outTuple = new TupleBuilder()
            .set("name", word)
            .set("value", increment)
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
        Increment increment = wordCounts.generateCounterSetIncrement(
            TwitterFlow.WORD_SET, Bytes.toBytes(word), 1L);
        collector.add(increment);
      }
    }

    // Track top users
    Increment increment = topUsers.generatePrimaryCounterIncrement(
        TwitterFlow.USER_SET, Bytes.toBytes(tweet.getUser()), 1L);
    Tuple outTuple = new TupleBuilder()
        .set("name", tweet.getUser())
        .set("value", increment)
        .create();
    
    collector.add(outTuple);
  }

}
