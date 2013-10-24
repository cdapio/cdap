package com.continuuity.common.queue;

import org.junit.Assert;
import org.junit.Test;

/**
 * tests that queue name works properly.
 */
public class QueueNameTest {

  @Test
  public void testQueueNameForFlowlet() {
    // create a queue name
    QueueName queueName = QueueName.fromFlowlet("app", "flow", "flowlet", "out");

    // verify all parts are correct
    Assert.assertTrue(queueName.isQueue());
    Assert.assertFalse(queueName.isStream());
    Assert.assertEquals("app", queueName.getFirstComponent());
    Assert.assertEquals("flow", queueName.getSecondComponent());
    Assert.assertEquals("flowlet", queueName.getThirdComponent());
    Assert.assertEquals("out", queueName.getSimpleName());

    // convert to URI and back, verify again
    queueName = QueueName.from(queueName.toURI());
    Assert.assertTrue(queueName.isQueue());
    Assert.assertFalse(queueName.isStream());
    Assert.assertEquals("app", queueName.getFirstComponent());
    Assert.assertEquals("flow", queueName.getSecondComponent());
    Assert.assertEquals("flowlet", queueName.getThirdComponent());
    Assert.assertEquals("out", queueName.getSimpleName());

    // convert to bytes and back, verify again
    queueName = QueueName.from(queueName.toBytes());
    Assert.assertTrue(queueName.isQueue());
    Assert.assertFalse(queueName.isStream());
    Assert.assertEquals("app", queueName.getFirstComponent());
    Assert.assertEquals("flow", queueName.getSecondComponent());
    Assert.assertEquals("flowlet", queueName.getThirdComponent());
    Assert.assertEquals("out", queueName.getSimpleName());
  }

  @Test
  public void testQueueNameForStream() {
    // create a queue name
    QueueName queueName = QueueName.fromStream("mystream");
    verifyStreamName(queueName, "mystream");

    queueName = QueueName.fromStream("audi_test_stream");
    verifyStreamName(queueName, "audi_test_stream");

    queueName = QueueName.fromStream("audi_-test_stream");
    verifyStreamName(queueName, "audi_-test_stream");
  }

  private void verifyStreamName(QueueName queueName, String streamName) {
    // verify all parts are correct
    Assert.assertFalse(queueName.isQueue());
    Assert.assertTrue(queueName.isStream());
    Assert.assertEquals(streamName, queueName.getFirstComponent());
    Assert.assertNull(queueName.getSecondComponent());
    Assert.assertNull(queueName.getThirdComponent());
    Assert.assertEquals(streamName, queueName.getSimpleName());

    // convert to URI and back, verify again
    queueName = QueueName.from(queueName.toURI());
    Assert.assertFalse(queueName.isQueue());
    Assert.assertTrue(queueName.isStream());
    Assert.assertEquals(streamName, queueName.getFirstComponent());
    Assert.assertNull(queueName.getSecondComponent());
    Assert.assertNull(queueName.getThirdComponent());
    Assert.assertEquals(streamName, queueName.getSimpleName());

    // convert to bytes and back, verify again
    queueName = QueueName.from(queueName.toBytes());
    Assert.assertFalse(queueName.isQueue());
    Assert.assertTrue(queueName.isStream());
    Assert.assertEquals(streamName, queueName.getFirstComponent());
    Assert.assertNull(queueName.getSecondComponent());
    Assert.assertNull(queueName.getThirdComponent());
    Assert.assertEquals(streamName, queueName.getSimpleName());
  }
}
