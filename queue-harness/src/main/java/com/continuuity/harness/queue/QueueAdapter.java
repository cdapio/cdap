/**
 * Copyright (c) 2012 to Continuuity Inc. All rights reserved.
 */
package com.continuuity.harness.queue;

import com.continuuity.queue.IQueue;
import com.continuuity.queue.QueueResult;

import java.io.IOException;

/**
 *  QueueAdapter contains an instance of the Queue class that it wraps
 *  All the calls to Queue class are funnelled through the adapter for
 *  additional functionality for measuring the performance.
 */
public class QueueAdapter {
  private final IQueue queue;
  private final byte[] name;

  /**
   * Constructor
   * @param name Name of the queue
   * @param queue  Queue Instance that Adapter will manage
   */
  public QueueAdapter(String name, IQueue queue ) {
    this.name = name.getBytes();
    this.queue = queue;
  }

  /**
   * Writes the data on to the queue.
   * @param data  Data to be written to queue.
   * @return true if successfull, else false.
   */
  public boolean enqueue(byte[] data) {
    boolean status = true;
    try {
      queue.enqueue(name, data);
    } catch (IOException e) {
      status = false;
    }
    return status;
  }

  /**
   * Reads a message from the queue.
   * @param consumerId ID of the consumer who is reading
   * @return byte array if successfull, else null.
   */
  public byte[] dequeue (byte[] consumerId) {
    QueueResult result = null;
    try {
      while(result == null) {
        result = queue.dequeue(consumerId, name);
      }
    } catch (IOException e) {
      return null;
    }
    return result.getData();
  }

}
