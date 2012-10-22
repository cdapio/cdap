/*
 * twitterScanner - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.examples.twitter;

import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.SourceFlowlet;
import com.continuuity.api.flow.flowlet.StreamsConfigurator;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;

/**
 * TwitterGenerator is a simple SourceFlowlet that pulls tweets from the Twitter
 * public timeline, wraps them in a Tuple and sends them to the next Flowlet
 * in our Flow.
 */
public class TwitterGenerator extends SourceFlowlet {

  /**
   * Configures a flowlet's streams. Basically, we define what our output
   * schema is going to be so that downstream flowlets can adhere to our
   * contract.
   *
   * @param configurator Stream configuration
   */
  @Override
  public void configure(StreamsConfigurator configurator) {

    // Configure the Schema for our output stream
    configurator.getDefaultTupleOutputStream().setSchema(
        TwitterFlow.TWEET_SCHEMA);

  }

  /**
   * This is the Twitter Client we will use to generate Tuples
   */
  private TwitterStream twitterStream;
  
  public StatusListener statusListener = new StatusListener() {
    @Override
    public void onStatus(Status status) {
      tweetQueue.offer(new Tweet(status));
    }
    @Override public void onException(Exception arg0) {}
    @Override public void onDeletionNotice(StatusDeletionNotice arg0) {}
    @Override public void onScrubGeo(long arg0, long arg1) {}
    @Override public void onTrackLimitationNotice(int arg0) {}    
  };

  @Override
  public void initialize() {

    tweetQueue = new LinkedBlockingQueue<Tweet>(1000);
    ConfigurationBuilder cb = new ConfigurationBuilder();
    cb.setUser(TwitterFlow.USERNAMES[0]).setPassword(TwitterFlow.PASSWORD);
    cb.setJSONStoreEnabled(true);
    cb.setDebugEnabled(false);
    twitter4j.conf.Configuration conf = cb.build();
    TwitterStreamFactory fact = new TwitterStreamFactory(conf);
    twitterStream = fact.getInstance();
    twitterStream.addListener(statusListener);
    twitterStream.sample();
  }

  private LinkedBlockingQueue<Tweet> tweetQueue;

  /**
   * This is the meat of this Flowlet. This is where we pull Tweets from
   * the Twitter public timeline, wrap them in Tuples and send them to
   * the next Flowlet(s) in our Flow.
   *
   * @param outputCollector  The Collector we'll use to send Tuples
   */
  @Override
  public void generate(OutputCollector outputCollector) {
    Tweet tweet;
    try {
      tweet = tweetQueue.take();
    } catch (InterruptedException e) {
      return;
    }
    Tuple tuple = new TupleBuilder().set("tweet", tweet).create();
    outputCollector.add(tuple);
  }

}
