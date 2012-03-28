/**
 * Copyright (c) 2012 to Continuuity Inc. All rights reserved.
 */
package com.continuuity.harness.queue;

import etm.core.configuration.EtmManager;
import etm.core.monitor.EtmMonitor;
import etm.core.monitor.EtmPoint;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

/**
 * Producer is responsible for generating the messages of a specified size
 * and enqueue on to a queue managed by a QueueAdapter. Producer is specified
 * to generate only a certain number of messages.
 */
public class Producer extends Thread {
  private static final EtmMonitor etmMonitor = EtmManager.getEtmMonitor();

  /**
   * Size of each message
   */
  private final int payloadSize;

  /**
   * Specifies if a producer is running or no
   */
  private volatile boolean running;

  /**
   * Handler to the queue
   */
  private final QueueAdapter adapter;

  /**
   * Random number generator
   */
  private Random random = new Random();

  /**
   * Optimization to hold the random data that is generated as message.
   * Additional eight bytes are added to store the long - message id.
   */
  private byte[] data;

  /**
   * Current message ID that is being generated and sent.
   */
  private long messageId;

  /**
   * Number of messages the producer needs to deliver.
   */
  private int messageLimit;

  /**
   * Streams for encoding the data before being put on queue.
   */
  private final ByteArrayOutputStream bos;
  private final DataOutputStream dos;

  /**
   * Constructor
   * @param adapter         Queue Adapter
   * @param payloadSize   Size of each message
   * @param messageLimit  Number of messages
   */
  Producer(QueueAdapter adapter, int payloadSize, int messageLimit) {
    this.payloadSize = payloadSize;
    this.running = true;
    this.adapter = adapter;
    this.data = new byte[payloadSize+8];
    this.messageId = 0;
     bos = new ByteArrayOutputStream();
    dos = new DataOutputStream(bos);
    this.messageLimit = messageLimit;
  }


  /**
   * @return true, if more messages need to be send, else false.
   */
  public boolean moreMessages() {
    if(messageId > messageLimit) return false;
    return true;
  }

  /**
   * Generates the next message. Adds message ID
   * @return
   */
  private byte[] nextMessage() {
    byte[] message = null;
    ++messageId;
    try {
      bos.reset();
      dos.writeLong(messageId);
      random.nextBytes(data);
      dos.write(data);
      message = bos.toByteArray();
    } catch (IOException e) {}
    return message;
  }

  /**
   * Thread main that would generate as many specified messages and then
   * exits.
   */
  public void run() {
    while(moreMessages()) {

      // Generate a message and measure the time it takes to generate
      // a message.
      EtmPoint pointOverhead = etmMonitor.createPoint("p:o:" + (messageId+1));
      byte[] message = nextMessage();
      pointOverhead.collect();
      
      if(message == null) continue;

      // Add a message on to the queue
      EtmPoint pointQueue = etmMonitor.createPoint("p:m:" + messageId);
      adapter.enqueue(message);
      pointQueue.collect();
    }
  }

}
