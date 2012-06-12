package com.continuuity.gateway;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import com.continuuity.api.flow.flowlet.Event;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.flow.flowlet.impl.EventBuilder;

public class ConsumerTest {

  /**
   * This tests that testConsumer counts correctly
   */
  @Test
  public void testConsumer() throws Exception {
    Consumer consumer = new Consumer() {
      @Override
      protected void single(Event event) throws Exception {
        if (event.hasHeader("dummy")) {
          throw new Exception("dummy header found");
        }
      }
    };
    consumer.configure(CConfiguration.create());

    Event goodEvent = new EventBuilder().setHeader("goody", "ok").create();
    Event badEvent = new EventBuilder().setHeader("dummy", "not ok").create();
    ArrayList<Event> goodBatch = new ArrayList<Event>();
    goodBatch.add(goodEvent);
    goodBatch.add(goodEvent);
    ArrayList<Event> badBatch = new ArrayList<Event>();
    badBatch.add(badEvent);
    badBatch.add(badEvent);
    ArrayList<Event> mixedBatch = new ArrayList<Event>();
    mixedBatch.add(goodEvent);
    mixedBatch.add(badEvent);

    // Start the consumer and verify that all counters are zero
    consumer.startConsumer();
    Assert.assertEquals(0, consumer.callsReceived());
    Assert.assertEquals(0, consumer.eventsReceived());
    Assert.assertEquals(0, consumer.callsSucceeded());
    Assert.assertEquals(0, consumer.eventsSucceeded());
    Assert.assertEquals(0, consumer.callsFailed());
    Assert.assertEquals(0, consumer.eventsFailed());

    // send a good event and verify counters went up by one
    consumer.consumeEvent(goodEvent);
    Assert.assertEquals(1, consumer.callsReceived());
    Assert.assertEquals(1, consumer.eventsReceived());
    Assert.assertEquals(1, consumer.callsSucceeded());
    Assert.assertEquals(1, consumer.eventsSucceeded());
    Assert.assertEquals(0, consumer.callsFailed());
    Assert.assertEquals(0, consumer.eventsFailed());

    // send a bad event and verify counters went up by one
    try {
      consumer.consumeEvent(badEvent);
    } catch (Exception e) {
    }
    Assert.assertEquals(2, consumer.callsReceived());
    Assert.assertEquals(2, consumer.eventsReceived());
    Assert.assertEquals(1, consumer.callsSucceeded());
    Assert.assertEquals(1, consumer.eventsSucceeded());
    Assert.assertEquals(1, consumer.callsFailed());
    Assert.assertEquals(1, consumer.eventsFailed());

    // send a batch of good events and verify counters went up
    consumer.consumeEvents(goodBatch);
    Assert.assertEquals(3, consumer.callsReceived());
    Assert.assertEquals(4, consumer.eventsReceived());
    Assert.assertEquals(2, consumer.callsSucceeded());
    Assert.assertEquals(3, consumer.eventsSucceeded());
    Assert.assertEquals(1, consumer.callsFailed());
    Assert.assertEquals(1, consumer.eventsFailed());

    // send a batch of bad events and verify counters went up
    try {
      consumer.consumeEvents(badBatch);
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
      consumer.consumeEvents(mixedBatch);
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
