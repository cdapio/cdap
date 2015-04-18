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

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.StageSpecification;
import co.cask.cdap.templates.etl.api.realtime.SourceContext;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.common.NoopMetrics;
import co.cask.cdap.templates.etl.common.Tweet;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TwitterStreamSourceTest {

  //NOTE: This test is ignored as it tests the twitter integration
  //In-order to test twitter API please pass in required credentials in the getRuntimeArguments method
  @Ignore
  @Test
  public void testIntegratedTwitterStream() throws Exception {
    TwitterStreamSource source = new TwitterStreamSource();
    source.configure(new StageConfigurer() {

      @Override
      public void setName(String name) {
        // No-op
      }

      @Override
      public void setDescription(String description) {
        // No-op
      }

      @Override
      public void addProperties(List<Property> properties) {
        // No-op
      }

      @Override
      public void addProperty(Property property) {
        // No-op
      }
    });

    source.initialize(new SourceContext() {
      @Override
      public StageSpecification getSpecification() {
        return null;
      }

      @Override
      public Metrics getMetrics() {
        return NoopMetrics.INSTANCE;
      }

      @Override
      public int getInstanceId() {
        return 0;
      }

      @Override
      public int getInstanceCount() {
        return 0;
      }

      @Override
      public Map<String, String> getRuntimeArguments() {
        Map<String, String> args = Maps.newHashMap();
        // NOTE: To get the valid credentials for testing please visit
        // https://dev.twitter.com/oauth/reference/post/oauth2/token
        // to get OAuth Consumer Key, Consumer Secret, Access Token and Access Token Secret
        args.put("ConsumerKey", "dummy");
        args.put("ConsumerSecret", "dummy");
        args.put("AccessToken", "dummy");
        args.put("AccessTokenSecret", "dummy");
        return args;
      }
    });

    MockEmitter emitter = new MockEmitter();
    SourceState state = new SourceState();


    Tweet tweet = getWithRetries(source, emitter, state, 10);
    Assert.assertNotNull(tweet);
  }


  private Tweet getWithRetries(TwitterStreamSource source, MockEmitter emitter,
                               SourceState state, int retryCount) throws Exception {

    Tweet tweet = null;
    int count = 0;
    while (count <= retryCount) {
      count++;
      tweet = emitter.getTweet();
      if (tweet != null) {
        break;
      }
      source.poll(emitter, state);
      TimeUnit.SECONDS.sleep(1L);
    }

    return tweet;
  }

  private static class MockEmitter implements Emitter<Tweet> {

    private Tweet tweet;

    @Override
    public void emit(Tweet value) {
      tweet = value;
    }

    public Tweet getTweet() {
      return tweet;
    }

  }
}
