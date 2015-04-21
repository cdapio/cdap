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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.realtime.RealtimeContext;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Date;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nullable;

/**
 * Implementation of {@link RealtimeSource} that reads data from Twitter API.
 * Users should pass in the following runtime arguments with appropriate OAuth credentials
 * ConsumerKey, ConsumerSecret, AccessToken, AccessTokenSecret.
 */
public class TwitterStreamSource extends RealtimeSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(TwitterStreamSource.class);

  private static final String ID = "id";
  private static final String MSG = "message";
  private static final String LANG = "lang";
  private static final String TIME = "time";
  private static final String FAVC = "favCount";
  private static final String RTC = "rtCont";
  private static final String SRC = "source";
  private static final String GLAT = "geolat";
  private static final String GLNG = "goLong";
  private static final String ISRT = "isRetweet";

  private TwitterStream twitterStream;
  private StatusListener statusListener;
  private Queue<Status> tweetQ = new ConcurrentLinkedQueue<Status>();

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

  private StructuredRecord convertTweet(Status tweet) {
    Schema.Field idField = Schema.Field.of(ID, Schema.of(Schema.Type.LONG));
    Schema.Field msgField = Schema.Field.of(MSG, Schema.of(Schema.Type.STRING));
    Schema.Field langField = Schema.Field.of(LANG, Schema.of(Schema.Type.STRING));
    Schema.Field timeField = Schema.Field.of(TIME, Schema.of(Schema.Type.LONG));
    Schema.Field favCount = Schema.Field.of(FAVC, Schema.of(Schema.Type.INT));
    Schema.Field rtCount = Schema.Field.of(RTC, Schema.of(Schema.Type.INT));
    Schema.Field sourceField = Schema.Field.of(SRC, Schema.of(Schema.Type.STRING));
    Schema.Field geoLatField = Schema.Field.of(GLAT, Schema.of(Schema.Type.DOUBLE));
    Schema.Field geoLongField = Schema.Field.of(GLNG, Schema.of(Schema.Type.DOUBLE));
    Schema.Field reTweetField = Schema.Field.of(ISRT, Schema.of(Schema.Type.BOOLEAN));
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(Schema.recordOf(
      "tweet", idField, msgField, langField, timeField, favCount, rtCount, sourceField, geoLatField, geoLongField,
      reTweetField));

    recordBuilder.set(ID, tweet.getId());
    recordBuilder.set(MSG, tweet.getText());
    recordBuilder.set(LANG, tweet.getLang());
    recordBuilder.set(TIME, convertDataToTimeStamp(tweet.getCreatedAt()));
    recordBuilder.set(FAVC, tweet.getFavoriteCount());
    recordBuilder.set(RTC, tweet.getRetweetCount());
    if (tweet.getGeoLocation() != null) {
      recordBuilder.set(GLAT, tweet.getGeoLocation().getLatitude());
      recordBuilder.set(GLNG, tweet.getGeoLocation().getLongitude());
    } else {
      recordBuilder.set(GLAT, -1L);
      recordBuilder.set(GLNG, -1L);
    }
    recordBuilder.set(ISRT, tweet.isRetweet());
    return recordBuilder.build();
  }

  private long convertDataToTimeStamp(Date date) {
    long startTime = date.getTime() * 1000000;
    return System.nanoTime() - startTime;
  }

  @Nullable
  @Override
  public SourceState poll(Emitter<StructuredRecord> writer, SourceState currentState) {
    if (!tweetQ.isEmpty()) {
      Status status = tweetQ.remove();
      StructuredRecord tweet = convertTweet(status);
      LOG.error("Sending Tweet : {}", tweet);
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
        tweetQ.add(status);
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

  @Override
  public void destroy() {
    if (twitterStream != null) {
      twitterStream.shutdown();
    }
  }
}
