package com.continuuity.gateway;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.streamevent.DefaultStreamEvent;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ConsumerTest {

  /**
   * This tests that testConsumer counts correctly
   */
  @Test
  public void testConsumer() throws Exception {
    Consumer consumer = new Consumer() {
      @Override
      protected void single(StreamEvent event, String accountId) throws Exception {
        if (event.getHeaders().containsKey("dummy")) {
          throw new Exception("dummy header found");
        }
      }
    };
    consumer.configure(CConfiguration.create());

    Map<String, String> goodHeaders = new TreeMap<String, String>();
    goodHeaders.put("goody", "ok");
    StreamEvent goodEvent = new DefaultStreamEvent(goodHeaders, null);

    Map<String, String> badHeaders = new TreeMap<String, String>();
    badHeaders.put("dummy", "not ok");
    StreamEvent badEvent = new DefaultStreamEvent(badHeaders, null);

    List<StreamEvent> goodBatch = Lists.newArrayList(goodEvent, goodEvent);
    List<StreamEvent> badBatch = Lists.newArrayList(badEvent, badEvent);
    List<StreamEvent> mixedBatch = Lists.newArrayList(goodEvent, badEvent);

    // Start the consumer and verify that all counters are zero
    consumer.startConsumer();
    Assert.assertEquals(0, consumer.callsReceived());
    Assert.assertEquals(0, consumer.eventsReceived());
    Assert.assertEquals(0, consumer.callsSucceeded());
    Assert.assertEquals(0, consumer.eventsSucceeded());
    Assert.assertEquals(0, consumer.callsFailed());
    Assert.assertEquals(0, consumer.eventsFailed());

    // send a good event and verify counters went up by one
    String DEFAULT_ACCOUNT_ID = "default";
    consumer.consumeEvent(goodEvent, DEFAULT_ACCOUNT_ID);
    Assert.assertEquals(1, consumer.callsReceived());
    Assert.assertEquals(1, consumer.eventsReceived());
    Assert.assertEquals(1, consumer.callsSucceeded());
    Assert.assertEquals(1, consumer.eventsSucceeded());
    Assert.assertEquals(0, consumer.callsFailed());
    Assert.assertEquals(0, consumer.eventsFailed());

    // send a bad event and verify counters went up by one
    try {
      consumer.consumeEvent(badEvent, DEFAULT_ACCOUNT_ID);
    } catch (Exception e) {
    }
    Assert.assertEquals(2, consumer.callsReceived());
    Assert.assertEquals(2, consumer.eventsReceived());
    Assert.assertEquals(1, consumer.callsSucceeded());
    Assert.assertEquals(1, consumer.eventsSucceeded());
    Assert.assertEquals(1, consumer.callsFailed());
    Assert.assertEquals(1, consumer.eventsFailed());

    // send a batch of good events and verify counters went up
    consumer.consumeEvents(goodBatch, DEFAULT_ACCOUNT_ID);
    Assert.assertEquals(3, consumer.callsReceived());
    Assert.assertEquals(4, consumer.eventsReceived());
    Assert.assertEquals(2, consumer.callsSucceeded());
    Assert.assertEquals(3, consumer.eventsSucceeded());
    Assert.assertEquals(1, consumer.callsFailed());
    Assert.assertEquals(1, consumer.eventsFailed());

    // send a batch of bad events and verify counters went up
    try {
      consumer.consumeEvents(badBatch, DEFAULT_ACCOUNT_ID);
    } catch (Exception e) {
    }
    Assert.assertEquals(4, consumer.callsReceived());
    Assert.assertEquals(6, consumer.eventsReceived());
    Assert.assertEquals(2, consumer.callsSucceeded());
    Assert.assertEquals(3, consumer.eventsSucceeded());
    Assert.assertEquals(2, consumer.callsFailed());
    Assert.assertEquals(3, consumer.eventsFailed());

    // send a batch of mixed events and verify counters went up
    try {
      consumer.consumeEvents(mixedBatch, DEFAULT_ACCOUNT_ID);
    } catch (Exception e) {
    }
    Assert.assertEquals(5, consumer.callsReceived());
    Assert.assertEquals(8, consumer.eventsReceived());
    Assert.assertEquals(2, consumer.callsSucceeded());
    Assert.assertEquals(3, consumer.eventsSucceeded());
    Assert.assertEquals(3, consumer.callsFailed());
    Assert.assertEquals(5, consumer.eventsFailed());

    // stop the consumer, verify counters are still there
    consumer.stopConsumer();
    Assert.assertEquals(5, consumer.callsReceived());
    Assert.assertEquals(8, consumer.eventsReceived());
    Assert.assertEquals(2, consumer.callsSucceeded());
    Assert.assertEquals(3, consumer.eventsSucceeded());
    Assert.assertEquals(3, consumer.callsFailed());
    Assert.assertEquals(5, consumer.eventsFailed());

    // Restart the consumer and verify that all counters reset are zero
    consumer.startConsumer();
    Assert.assertEquals(0, consumer.callsReceived());
    Assert.assertEquals(0, consumer.eventsReceived());
    Assert.assertEquals(0, consumer.callsSucceeded());
    Assert.assertEquals(0, consumer.eventsSucceeded());
    Assert.assertEquals(0, consumer.callsFailed());
    Assert.assertEquals(0, consumer.eventsFailed());
  }
}
