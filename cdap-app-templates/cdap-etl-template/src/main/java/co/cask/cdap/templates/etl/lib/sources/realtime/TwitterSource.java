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

package co.cask.cdap.templates.etl.lib.sources.realtime;

import co.cask.cdap.templates.etl.api.SourceConfigurer;
import co.cask.cdap.templates.etl.api.SourceContext;
import co.cask.cdap.templates.etl.api.realtime.Emitter;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.api.stages.AbstractRealtimeSource;
import com.google.common.collect.Sets;
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
 *
 */
public class TwitterSource extends AbstractRealtimeSource<String> {

  private ConfigurationBuilder cb;
  private TwitterStream tStream;
  private StatusListener statusListener;
  private Queue<String> tweetQ = new ConcurrentLinkedQueue<String>();

  @Override
  public void configure(SourceConfigurer configurer) {
    super.configure(configurer);
    configurer.setDescription("Twitter Data Source. Sends only EN language Tweet text out");
    configurer.setMaxInstances(1);
    configurer.setReqdProperties(Sets.newHashSet("ConsumerKey", "ConsumerSecret",
                                                 "AccessToken", "AccessTokenSecret"));
  }

  @Nullable
  @Override
  public SourceState poll(Emitter<String> writer, @Nullable SourceState currentState) {
    if (!tweetQ.isEmpty()) {
      String tweet = tweetQ.remove();
      writer.emit(tweet);
    }
    return currentState;
  }

  @Override
  public void initialize(SourceContext context) {
    super.initialize(context);
    statusListener = new StatusListener() {
      @Override
      public void onStatus(Status status) {
        if (status.getLang().equalsIgnoreCase("en")) {
          tweetQ.add(status.getText());
        }
      }

      @Override
      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

      }

      @Override
      public void onTrackLimitationNotice(int i) {

      }

      @Override
      public void onScrubGeo(long l, long l1) {

      }

      @Override
      public void onStallWarning(StallWarning stallWarning) {

      }

      @Override
      public void onException(Exception e) {

      }
    };

    cb = new ConfigurationBuilder();
    cb.setDebugEnabled(false)
      .setOAuthConsumerKey(context.getRuntimeArguments().get("ConsumerKey"))
      .setOAuthConsumerSecret(context.getRuntimeArguments().get("ConsumerSecret"))
      .setOAuthAccessToken(context.getRuntimeArguments().get("AccessToken"))
      .setOAuthAccessTokenSecret(context.getRuntimeArguments().get("AccessTokenSecret"));

    tStream = new TwitterStreamFactory(cb.build()).getInstance();
    tStream.addListener(statusListener);
    tStream.sample();
  }

  @Override
  public void destroy() {
    tStream.shutdown();
  }
}
