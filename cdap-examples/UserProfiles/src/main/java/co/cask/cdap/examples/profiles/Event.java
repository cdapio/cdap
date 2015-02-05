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

package co.cask.cdap.examples.profiles;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.InputStreamReader;
import java.nio.ByteBuffer;

/**
 * An event represents a user's click on a URL.
 */
public class Event {

  private static final Gson GSON = new Gson();

  private final long time;
  private final String userId;
  private final String url;

  Event(long time, String userId, String url) {
    this.time = time;
    this.userId = userId;
    this.url = url;
  }

  public static Event fromJson(ByteBuffer input) {
    Event event = GSON.fromJson(new InputStreamReader(
      new ChannelBufferInputStream(
        ChannelBuffers.wrappedBuffer(input))), Event.class);
    Preconditions.checkNotNull(event.getTime(), "Time cannot be null.");
    Preconditions.checkNotNull(event.getUserId(), "User ID cannot be null.");
    Preconditions.checkNotNull(event.getUrl(), "URL cannot be null.");
    return event;
  }

  public long getTime() {
    return time;
  }

  public String getUserId() {
    return userId;
  }

  public String getUrl() {
    return url;
  }
}
