/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.templates.etl.realtime.sources;

import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.api.realtime.RealtimeContext;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.common.Tweet;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nullable;

/**
 * Implementation of {@link RealtimeSource} that reads data from Twitter API.
 * Users should pass in the following runtime arguments with appropriate OAuth credentials
 * ConsumerKey, ConsumerSecret, AccessToken, AccessTokenSecret.
 */
public class TwitterStreamSource extends RealtimeSource<Tweet> {
  private TwitterStream twitterStream;
  private StatusListener statusListener;
  private Queue<Tweet> tweetQ = new ConcurrentLinkedQueue<Tweet>();

  /**
   * Configure the Twitter Source.
   *
   * @param configurer {@link StageConfigurer}
   */
  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(TwitterStreamSource.class.getSimpleName());
    configurer.setDescription("Twitter Realtime Source");
    configurer.addProperty(new Property("ConsumerKey", "Consumer Key", true));
    configurer.addProperty(new Property("ConsumerSecret", "Consumer Secret", true));
    configurer.addProperty(new Property("AccessToken", "Access Token", true));
    configurer.addProperty(new Property("AccessTokenSecret", "Access Token Secret", true));
  }

  @Nullable
  @Override
  public SourceState poll(Emitter<Tweet> writer, SourceState currentState) {
    if (!tweetQ.isEmpty()) {
      Tweet tweet = tweetQ.remove();
      writer.emit(tweet);
    }
    return currentState;
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);

    statusListener = new StatusListener() {
      @Override
      public void onStatus(Status status) {
        tweetQ.add(new Tweet(status.getId(), status.getText(), status.getLang(), status.getCreatedAt(),
                             status.getFavoriteCount(), status.getRetweetCount(), status.getSource(),
                             status.getGeoLocation(), status.isRetweet()));
      }

      @Override
      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
       // No-op
      }

      @Override
      public void onTrackLimitationNotice(int i) {
       // No-op
      }

      @Override
      public void onScrubGeo(long l, long l1) {
        // No-op
      }

      @Override
      public void onStallWarning(StallWarning stallWarning) {
        // No-op
      }

      @Override
      public void onException(Exception e) {
        // No-op
      }
    };

    ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
    configurationBuilder.setDebugEnabled(false)
        .setOAuthConsumerKey(context.getRuntimeArguments().get("ConsumerKey"))
        .setOAuthConsumerSecret(context.getRuntimeArguments().get("ConsumerSecret"))
        .setOAuthAccessToken(context.getRuntimeArguments().get("AccessToken"))
        .setOAuthAccessTokenSecret(context.getRuntimeArguments().get("AccessTokenSecret"));

    twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();
    twitterStream.addListener(statusListener);
    twitterStream.sample();
  }
}
