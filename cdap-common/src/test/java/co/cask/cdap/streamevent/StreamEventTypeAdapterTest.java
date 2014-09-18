/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.streamevent;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.stream.DefaultStreamEvent;
import co.cask.cdap.common.stream.StreamEventTypeAdapter;
import com.google.common.base.Charsets;
import com.google.common.base.Equivalence;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Test for the Gson TypeAdapter for StreamEvent
 */
public class StreamEventTypeAdapterTest {

  // For comparing equivalence of StreamEvent objects
  private static final Equivalence<StreamEvent> STREAM_EVENT_EQUIVALENCE = new Equivalence<StreamEvent>() {

    @Override
    protected boolean doEquivalent(StreamEvent a, StreamEvent b) {
      return Objects.equal(a.getTimestamp(), b.getTimestamp())
        && Objects.equal(a.getHeaders(), b.getHeaders())
        && Objects.equal(a.getBody(), b.getBody());
    }

    @Override
    protected int doHash(StreamEvent streamEvent) {
      return Objects.hashCode(streamEvent.getTimestamp(), streamEvent.getHeaders(), streamEvent.getBody());
    }
  };

  @Test
  public void testAdapter() throws IOException {
    // Writes bunch of stream events, serialize them into json and read them. Check if the read back is the
    // same as the original list.

    Gson gson = StreamEventTypeAdapter.register(new GsonBuilder()).create();

    List<StreamEvent> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      events.add(new DefaultStreamEvent(ImmutableMap.of("k" + i, "v" + i), Charsets.UTF_8.encode("Msg " + i), i));
    }

    List<StreamEvent> decoded = gson.fromJson(gson.toJson(events), new TypeToken<List<StreamEvent>>() { }.getType());

    // The decoded events should be the same as the original events
    Iterator<StreamEvent> it1 = events.iterator();
    Iterator<StreamEvent> it2 = decoded.iterator();

    while (it1.hasNext() && it2.hasNext()) {
      Assert.assertTrue(STREAM_EVENT_EQUIVALENCE.equivalent(it1.next(), it2.next()));
    }
    // Both iterator should be emptied.
    Assert.assertEquals(it1.hasNext(), it2.hasNext());
  }
}
