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

package co.cask.cdap.template.etl.realtime.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.realtime.RealtimeContext;
import co.cask.cdap.template.etl.api.realtime.RealtimeSource;
import co.cask.cdap.template.etl.api.realtime.SourceState;
import com.google.common.collect.Queues;
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
import javax.annotation.Nullable;

/**
 * Implementation of {@link RealtimeSource} that reads data from Twitter API.
 * Users should pass in the following runtime arguments with appropriate OAuth credentials
 * ConsumerKey, ConsumerSecret, AccessToken, AccessTokenSecret.
 */
@Plugin(type = "source")
@Name("Twitter")
@Description("Twitter Realtime Source. Output records contain the following fields: " +
  "id (long), message (string), lang (nullable string), time (nullable long), favCount (int), " +
  "rtCount (int), source (nullable string), geoLat (nullable double), geoLong (nullable double), " +
  "isRetweet (boolean).")
public class TwitterSource extends RealtimeSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(TwitterSource.class);
  private static final String CONSUMER_KEY = "ConsumerKey";
  private static final String CONSUMER_SECRET = "ConsumerSecret";
  private static final String ACCESS_TOKEN = "AccessToken";
  private static final String ACCESS_SECRET = "AccessTokenSecret";

  private static final String ID = "id";
  private static final String MSG = "message";
  private static final String LANG = "lang";
  private static final String TIME = "time";
  private static final String FAVC = "favCount";
  private static final String RTC = "rtCount";
  private static final String SRC = "source";
  private static final String GLAT = "geoLat";
  private static final String GLNG = "geoLong";
  private static final String ISRT = "isRetweet";

  private TwitterStream twitterStream;
  private StatusListener statusListener;
  private Queue<Status> tweetQ = Queues.newConcurrentLinkedQueue();
  private Schema schema;

  private final TwitterConfig twitterConfig;

  public TwitterSource(TwitterConfig twitterConfig) {
    this.twitterConfig = twitterConfig;
  }

  /**
   * Config class for TwitterSource.
   */
  public static class TwitterConfig extends PluginConfig {

    @Name(CONSUMER_KEY)
    @Description("Consumer Key")
    private String consumerKey;

    @Name(CONSUMER_SECRET)
    @Description("Consumer Secret")
    private String consumeSecret;

    @Name(ACCESS_TOKEN)
    @Description("Access Token")
    private String accessToken;

    @Name(ACCESS_SECRET)
    @Description("Access Token Secret")
    private String accessTokenSecret;

    public TwitterConfig(String consumerKey, String consumeSecret, String accessToken, String accessTokenSecret) {
      this.consumerKey = consumerKey;
      this.consumeSecret = consumeSecret;
      this.accessToken = accessToken;
      this.accessTokenSecret = accessTokenSecret;
    }
  }

  private StructuredRecord convertTweet(Status tweet) {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(this.schema);
    recordBuilder.set(ID, tweet.getId());
    recordBuilder.set(MSG, tweet.getText());
    recordBuilder.set(LANG, tweet.getLang());
    Date tweetDate = tweet.getCreatedAt();
    if (tweetDate != null) {
      recordBuilder.set(TIME, tweetDate.getTime());
    }
    recordBuilder.set(FAVC, tweet.getFavoriteCount());
    recordBuilder.set(RTC, tweet.getRetweetCount());
    recordBuilder.set(SRC, tweet.getSource());
    if (tweet.getGeoLocation() != null) {
      recordBuilder.set(GLAT, tweet.getGeoLocation().getLatitude());
      recordBuilder.set(GLNG, tweet.getGeoLocation().getLongitude());
    }
    recordBuilder.set(ISRT, tweet.isRetweet());
    return recordBuilder.build();
  }

  @Nullable
  @Override
  public SourceState poll(Emitter<StructuredRecord> writer, SourceState currentState) {
    if (!tweetQ.isEmpty()) {
      Status status = tweetQ.remove();
      StructuredRecord tweet = convertTweet(status);
      writer.emit(tweet);
    }
    return currentState;
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);

    // Disable chatty logging from twitter4j.
    System.setProperty("twitter4j.loggerFactory", "twitter4j.NullLoggerFactory");

    Schema.Field idField = Schema.Field.of(ID, Schema.of(Schema.Type.LONG));
    Schema.Field msgField = Schema.Field.of(MSG, Schema.of(Schema.Type.STRING));
    Schema.Field langField = Schema.Field.of(LANG, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    Schema.Field timeField = Schema.Field.of(TIME, Schema.nullableOf(Schema.of(Schema.Type.LONG)));
    Schema.Field favCount = Schema.Field.of(FAVC, Schema.of(Schema.Type.INT));
    Schema.Field rtCount = Schema.Field.of(RTC, Schema.of(Schema.Type.INT));
    Schema.Field sourceField = Schema.Field.of(SRC, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
    Schema.Field geoLatField = Schema.Field.of(GLAT, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)));
    Schema.Field geoLongField = Schema.Field.of(GLNG, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)));
    Schema.Field reTweetField = Schema.Field.of(ISRT, Schema.of(Schema.Type.BOOLEAN));
    schema = Schema.recordOf("tweet", idField, msgField, langField, timeField, favCount, rtCount, sourceField,
                             geoLatField, geoLongField, reTweetField);

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
        .setOAuthConsumerKey(twitterConfig.consumerKey)
        .setOAuthConsumerSecret(twitterConfig.consumeSecret)
        .setOAuthAccessToken(twitterConfig.accessToken)
        .setOAuthAccessTokenSecret(twitterConfig.accessTokenSecret);

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
