/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.continuuity.examples.twitter;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.continuuity.api.annotation.Tick;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;



/**
 * TwitterGenerator is a simple SourceFlowlet that pulls tweets from the Twitter
 * public timeline, wraps them in a Tuple, and sends them to the next flowlet
 * in our flow.
 */
public class TwitterGenerator extends AbstractFlowlet {

  public StatusListener statusListener = new StatusListener() {
    @Override
    public void onStatus(Status status) {
      tweetQueue.offer(new Tweet(status));
    }

    @Override
    public void onException(Exception arg0) {
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice arg0) {
    }

    @Override
    public void onScrubGeo(long arg0, long arg1) {
    }

    @Override
    public void onTrackLimitationNotice(int arg0) {
    }
  };
  private OutputEmitter<Tweet> output;
  /**
   * This is the Twitter client we will use to generate tuples.
   */
  private TwitterStream twitterStream;
  private LinkedBlockingQueue<Tweet> tweetQueue;

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
   * This is the main method of the flowlet. It pulls from the internal queue
   * of tweets (filled using twitter4j callback setup in initialize()) and emits
   * them to the next flowlet.
   */
  @Tick(delay = 1L, unit = TimeUnit.MILLISECONDS)
  public void generate() throws InterruptedException {
    Tweet tweet = tweetQueue.take();
    output.emit(tweet);
  }

}
