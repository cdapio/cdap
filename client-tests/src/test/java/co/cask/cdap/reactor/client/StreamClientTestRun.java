/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.reactor.client;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.exception.BadRequestException;
import co.cask.cdap.client.exception.StreamNotFoundException;
import co.cask.cdap.client.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.reactor.client.common.ClientTestBase;
import co.cask.cdap.test.XSlowTests;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link StreamClient}.
 */
@Category(XSlowTests.class)
public class StreamClientTestRun extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(StreamClientTestRun.class);

  private StreamClient streamClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    streamClient = new StreamClient(clientConfig);
  }

  @Test
  public void testAll() throws Exception {
    String testStreamId = "teststream";

    LOG.info("Getting stream list");
    int baseStreamCount = streamClient.list().size();
    Assert.assertEquals(baseStreamCount, streamClient.list().size());
    LOG.info("Creating stream");
    streamClient.create(testStreamId);
    LOG.info("Checking stream list");
    Assert.assertEquals(baseStreamCount + 1, streamClient.list().size());
    StreamProperties config = streamClient.getConfig(testStreamId);
    Assert.assertNotNull(config);
    Assert.assertEquals(testStreamId, config.getName());
    // TODO: getting and setting config for stream is not supported with in-memory
//    streamClient.setTTL(testStreamId, 123);
//    streamClient.sendEvent(testStreamId, testStreamEvent);
//    streamClient.truncate(testStreamId);
//    streamClient.sendEvent(testStreamId, testStreamEvent);
//    String consumerId = streamClient.getConsumerId(testStreamId);
//    Assert.assertEquals(testStreamEvent, streamClient.dequeueEvent(testStreamId, consumerId));
//    Assert.assertEquals(null, streamClient.dequeueEvent(testStreamId, consumerId));
  }

  /**
   * Tests for the get events call
   */
  @Test
  public void testStreamEvents() throws IOException, BadRequestException, StreamNotFoundException,
    UnAuthorizedAccessTokenException {

    String streamId = "testEvents";

    streamClient.create(streamId);
    for (int i = 0; i < 10; i++) {
      streamClient.sendEvent(streamId, "Testing " + i);
    }

    // Read all events
    List<StreamEvent> events = streamClient.getEvents(streamId, 0, Long.MAX_VALUE,
                                                      Integer.MAX_VALUE, Lists.<StreamEvent>newArrayList());
    Assert.assertEquals(10, events.size());

    // Read first 5 only
    events.clear();
    streamClient.getEvents(streamId, 0, Long.MAX_VALUE, 5, events);
    Assert.assertEquals(5, events.size());

    // Read 2nd and 3rd only
    long startTime = events.get(1).getTimestamp();
    long endTime = events.get(2).getTimestamp() + 1;
    events.clear();
    streamClient.getEvents(streamId, startTime, endTime, Integer.MAX_VALUE, events);

    Assert.assertEquals(2, events.size());

    for (int i = 1; i < 3; i++) {
      Assert.assertEquals("Testing " + i, Charsets.UTF_8.decode(events.get(i - 1).getBody()).toString());
    }
  }

  /**
   * Tests for async write to stream.
   */
  @Test
  public void testAsyncWrite() throws Exception {
    String streamId = "testAsync";

    streamClient.create(streamId);

    // Send 10 async writes
    int msgCount = 10;
    for (int i = 0; i < msgCount; i++) {
      streamClient.asyncSendEvent(streamId, "Testing " + i);
    }

    // Reads them back to verify. Needs to do it multiple times as the writes happens async.
    List<StreamEvent> events = Lists.newArrayList();

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    while (events.size() != msgCount && stopwatch.elapsedTime(TimeUnit.SECONDS) < 10L) {
      events.clear();
      streamClient.getEvents(streamId, 0, Long.MAX_VALUE, msgCount, events);
    }

    Assert.assertEquals(msgCount, events.size());
    long lastTimestamp = 0L;
    for (int i = 0; i < msgCount; i++) {
      Assert.assertEquals("Testing " + i, Charsets.UTF_8.decode(events.get(i).getBody()).toString());
      lastTimestamp = events.get(i).getTimestamp();
    }

    // No more events
    stopwatch = new Stopwatch();
    stopwatch.start();
    events.clear();
    while (events.isEmpty() && stopwatch.elapsedTime(TimeUnit.SECONDS) < 1L) {
      events.clear();
      streamClient.getEvents(streamId, lastTimestamp + 1, Long.MAX_VALUE, msgCount, events);
    }

    Assert.assertTrue(events.isEmpty());
  }
}
