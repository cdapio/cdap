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

import com.continuuity.api.flow.flowlet.AbstractGeneratorFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;

/**
 * TwitterGenerator is a simple SourceFlowlet that pulls tweets from the Twitter
 * public timeline, wraps them in a Tuple and sends them to the next Flowlet
 * in our Flow.
 */
public class TwitterGenerator extends AbstractGeneratorFlowlet {

  private OutputEmitter<Tweet> output;

  /**
   * This is the Twitter Client we will use to generate Tuples
   */
  private TwitterStream twitterStream;

  private LinkedBlockingQueue<Tweet> tweetQueue;

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
  public void initialize(FlowletContext context) {
    tweetQueue = new LinkedBlockingQueue<Tweet>(1000);
    ConfigurationBuilder cb = new ConfigurationBuilder();
    cb.setUser(TwitterScanner.USERNAMES[0]);
    cb.setPassword(TwitterScanner.PASSWORD);
    cb.setJSONStoreEnabled(true);
    cb.setDebugEnabled(false);
    twitter4j.conf.Configuration conf = cb.build();
    TwitterStreamFactory fact = new TwitterStreamFactory(conf);
    twitterStream = fact.getInstance();
    twitterStream.addListener(statusListener);
    twitterStream.sample();
  }

  /**
   * This is the main method of the flowlet.  It pulls from the internal queue
   * of tweets (filled using twitter4j callback setup in initialize()) and emits
   * them to the next flowlet.
   */
  @Override
  public void generate() throws InterruptedException {
    Tweet tweet = tweetQueue.take();
    output.emit(tweet);
  }

}
