/**
 * Copyright (c) 2012 to Continuuity Inc. All rights reserved.
 */
package com.continuuity.harness.queue;

import etm.core.configuration.EtmManager;
import etm.core.monitor.EtmMonitor;
import etm.core.monitor.EtmPoint;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Simple Consumer that is responsible for picking up a message from
 * the queue that it is assigned to be pulled from.
 */
public class Consumer extends  Thread {
  private static final EtmMonitor etmMonitor = EtmManager.getEtmMonitor();

  /**
   * Specifies whether the thread is running or no
   */
  private volatile boolean running;

  /**
   * Id of the consumer.
   */
  private final byte[]  id;

  /**
   * Handle to QueueAdapter
   */
  private final QueueAdapter adapter;

  /**
   * Max number of message the consumer should see before calling it done
   */
  private final int msgLimit;

  /**
   * Number of messages as seen by the consumer.
   */
  private int messageId;

  /**
   * Constructor
   * @param id            ID of the consumer
   * @param adapter   Handle to Queue Adapter
   * @param msgLimit Number of message the consumer should expect
   */
  Consumer(String id, QueueAdapter adapter, int msgLimit) {
    this.id = id.getBytes();
    this.running = true;
    this.adapter = adapter;
    this.msgLimit = msgLimit;
    this.messageId = 0;
  }

  /**
   * @return  Returns true if consumer is expecting more messages
   */
  public boolean moreMessages() {
    if(messageId > msgLimit) return false;
    return true;
  }

  /**
   *  Thread running the test - consuming messages from the queue
   *  using the queue adapter handle and deserializing the payload.
   */
  public void run() {
    while(moreMessages()) {
      EtmPoint pointQueue = etmMonitor.createPoint("c:m");
      byte[]  data = adapter.dequeue(id);
      EtmPoint pointOverhead = etmMonitor.createPoint("c:o");
      ByteArrayInputStream bis = new ByteArrayInputStream(data);
      DataInputStream dis = new DataInputStream(bis);
      try {
        long id = dis.readLong();
        pointQueue.alterName("c:m:" + id);
        pointOverhead.alterName("c:o:" + id);
        messageId++;
        pointQueue.collect();
        pointOverhead.collect();
      } catch (IOException e){}
    }
  }

}
