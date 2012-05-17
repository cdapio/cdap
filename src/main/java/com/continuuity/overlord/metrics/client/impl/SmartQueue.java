/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.overlord.metrics.client.impl;

import com.continuuity.overlord.metrics.client.MetricConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;

/**
 *  A smart queue for collecting all client generated
 *  to be submitted to sink.  It's a producer biased queue.
 */
public class SmartQueue<T> {
  private static final Logger LOG = LoggerFactory.getLogger(SmartQueue.class);
  private final T[] data;
  private int head;
  private int tail;
  private int size;
  private Thread currentConsumer = null;

  SmartQueue(int capacity) {
    this.data = (T[]) new Object[Math.max(1, capacity)];
    head = tail = size = 0;
  }

  synchronized boolean enqueue(T e) {
    if (data.length == size) {
      return false;
    }
    ++size;
    tail = (tail + 1) % data.length;
    data[tail] = e;
    notify();
    return true;
  }

  void consume(MetricConsumer<T> consumer) throws InterruptedException {
    T e = waitForData();

    try {
      consumer.consume(e);  // can take forever
      _dequeue();
    }
    finally {
      clearConsumerLock();
    }
  }

  void consumeAll(MetricConsumer<T> consumer) throws InterruptedException {
    waitForData();

    try {
      for (int i = size(); i-- > 0; ) {
        consumer.consume(front()); // can take forever
        _dequeue();
      }
    }
    finally {
      clearConsumerLock();
    }
  }

  synchronized T dequeue() throws InterruptedException {
    checkConsumer();

    while (0 == size) {
      wait();
    }
    return _dequeue();
  }

  private synchronized T waitForData() throws InterruptedException {
    checkConsumer();

    while (0 == size) {
      wait();
    }
    setConsumerLock();
    return front();
  }

  private synchronized void checkConsumer() {
    if (currentConsumer != null) {
      throw new ConcurrentModificationException("The "+
        currentConsumer.getName() +" thread is consuming the queue.");
    }
  }

  private synchronized void setConsumerLock() {
    currentConsumer = Thread.currentThread();
  }

  private synchronized void clearConsumerLock() {
    currentConsumer = null;
  }

  private synchronized T _dequeue() {
    if (0 == size) {
      throw new IllegalStateException("Size must > 0 here.");
    }
    --size;
    head = (head + 1) % data.length;
    T ret = data[head];
    data[head] = null;  // hint to gc
    return ret;
  }

  synchronized T front() {
    return data[(head + 1) % data.length];
  }

  synchronized T back() {
    return data[tail];
  }

  synchronized void clear() {
    checkConsumer();

    for (int i = data.length; i-- > 0; ) {
      data[i] = null;
    }
    size = 0;
  }

  synchronized int size() {
    return size;
  }

  int capacity() {
    return data.length;
  }

}
