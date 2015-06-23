/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.CannotBeDeletedException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.StreamNotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.test.XSlowTests;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link StreamClient}.
 */
@Category(XSlowTests.class)
public class StreamClientTestRun extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(StreamClientTestRun.class);
  private static final Id.Namespace namespaceId = Id.Namespace.from("myspace");

  private NamespaceClient namespaceClient;
  private StreamClient streamClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    namespaceClient = new NamespaceClient(clientConfig);
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespaceId).build());
    clientConfig.setNamespace(namespaceId);
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
  public void testStreamEvents() throws IOException, BadRequestException,
    StreamNotFoundException, UnauthorizedException {

    String streamId = "testEvents";

    streamClient.create(streamId);

    // Send 5000 events
    int eventCount = 5000;
    for (int i = 0; i < eventCount; i++) {
      streamClient.sendEvent(streamId, "Testing " + i);
    }

    // Read all events
    List<StreamEvent> events = streamClient.getEvents(streamId, 0, Long.MAX_VALUE,
                                                      Integer.MAX_VALUE, Lists.<StreamEvent>newArrayList());
    Assert.assertEquals(eventCount, events.size());
    for (int i = 0; i < eventCount; i++) {
      Assert.assertEquals("Testing " + i, Bytes.toString(events.get(i).getBody()));
    }

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

  @Test
  public void testSendSmallFile() throws Exception {
    testSendFile(50);
  }

  @Test
  public void testSendLargeFile() throws Exception {
    testSendFile(500000);
  }

  @Test
  public void testDelete() throws Exception {
    String streamId = "testDelete";
    streamClient.create(streamId);

    // Send an event and get it back
    String msg = "Test Delete";
    streamClient.sendEvent(streamId, msg);
    List<StreamEvent> events = Lists.newArrayList();
    streamClient.getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE, events);
    Assert.assertEquals(1, events.size());
    Assert.assertEquals(msg, Charsets.UTF_8.decode(events.get(0).getBody()).toString());

    // Delete the stream
    streamClient.delete(streamId);
    // Try to get info, it should throw a StreamNotFoundException
    try {
      streamClient.getConfig(streamId);
      Assert.fail();
    } catch (StreamNotFoundException e) {
      // Expected
    }

    // Try to get events, it should throw a StreamNotFoundException
    try {
      streamClient.getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE, events);
      Assert.fail();
    } catch (StreamNotFoundException e) {
      // Expected
    }

    // Create the stream again, it should returns empty events
    streamClient.create(streamId);
    events.clear();
    streamClient.getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE, events);
    Assert.assertTrue(events.isEmpty());
  }


  private void testSendFile(int msgCount) throws Exception {
    String streamId = "testSendFile";

    streamClient.create(streamId);

    // Generate msgCount lines of events
    StringWriter writer = new StringWriter();
    for (int i = 0; i < msgCount; i++) {
      writer.write("Event " + i);
      writer.write("\n");
    }
    streamClient.sendBatch(streamId, "text/plain",
                           ByteStreams.newInputStreamSupplier(writer.toString().getBytes(Charsets.UTF_8)));

    // Reads the msgCount events back
    List<StreamEvent> events = Lists.newArrayList();
    streamClient.getEvents(streamId, 0, Long.MAX_VALUE, Integer.MAX_VALUE, events);

    Assert.assertEquals(msgCount, events.size());

    for (int i = 0; i < msgCount; i++) {
      StreamEvent event = events.get(i);
      Assert.assertEquals("Event " + i, Bytes.toString(event.getBody()));
      Assert.assertEquals("text/plain", event.getHeaders().get("content.type"));
    }
  }

  @After
  public void tearDown() throws CannotBeDeletedException, UnauthorizedException, NotFoundException, IOException {
    namespaceClient.delete(namespaceId.getId());
    clientConfig.setNamespace(namespaceId);
  }
}
