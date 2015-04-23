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
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.common.MockRealtimeContext;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
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
      public void addProperties(Collection<Property> properties) {
        // No-op
      }

      @Override
      public void addProperty(Property property) {
        // No-op
      }
    });

    Map<String, String> args = Maps.newHashMap();
    // NOTE: To get the valid credentials for testing please visit
    // https://dev.twitter.com/oauth/reference/post/oauth2/token
    // to get OAuth Consumer Key, Consumer Secret, Access Token and Access Token Secret
    args.put("ConsumerKey", "dummy");
    args.put("ConsumerSecret", "dummy");
    args.put("AccessToken", "dummy");
    args.put("AccessTokenSecret", "dummy");
    source.initialize(new MockRealtimeContext(args));

    MockEmitter emitter = new MockEmitter();
    SourceState state = new SourceState();


    StructuredRecord tweet = getWithRetries(source, emitter, state, 10);
    Assert.assertNotNull(tweet);
  }


  private StructuredRecord getWithRetries(TwitterStreamSource source, MockEmitter emitter,
                                          SourceState state, int retryCount) throws Exception {

    StructuredRecord tweet = null;
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

  private static class MockEmitter implements Emitter<StructuredRecord> {

    private StructuredRecord tweet;

    @Override
    public void emit(StructuredRecord value) {
      tweet = value;
    }

    public StructuredRecord getTweet() {
      return tweet;
    }

  }
}
