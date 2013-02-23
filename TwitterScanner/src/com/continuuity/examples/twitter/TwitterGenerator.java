/*
 * twitterScanner - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.examples.twitter;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.GeneratorFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * TwitterGenerator is a simple SourceFlowlet that pulls tweets from the Twitter
 * public timeline, wraps them in a Tuple and sends them to the next Flowlet
 * in our Flow.
 */
public class TwitterGenerator extends AbstractFlowlet implements GeneratorFlowlet {

  private OutputEmitter<Tweet> output;

  public TwitterGenerator() {
    super("StreamReader");
  }
  /**
   * Configures a flowlet's streams. Basically, we define what our output
   * schema is going to be so that downstream flowlets can adhere to our
   * contract.
   *
   * @param specifier Flowlet specifier.
   */

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

//  @Override
//  public void initialize() {
//
//    tweetQueue = new LinkedBlockingQueue<Tweet>(1000);
//    ConfigurationBuilder cb = new ConfigurationBuilder();
//    cb.setUser(TwitterFlow.USERNAMES[0]).setPassword(TwitterFlow.PASSWORD);
//    cb.setJSONStoreEnabled(true);
//    cb.setDebugEnabled(false);
//    twitter4j.conf.Configuration conf = cb.build();
//    TwitterStreamFactory fact = new TwitterStreamFactory(conf);
//    twitterStream = fact.getInstance();
//    twitterStream.addListener(statusListener);
//    twitterStream.sample();
//  }


  /**
   * This is the meat of this Flowlet. This is where we pull Tweets from
   * the Twitter public timeline, wrap them in Tuples and send them to
   * the next Flowlet(s) in our Flow.
   */
  @Override
  public void generate() throws InterruptedException {
    Tweet tweet = tweetQueue.take();
    output.emit(tweet);
  }

}
