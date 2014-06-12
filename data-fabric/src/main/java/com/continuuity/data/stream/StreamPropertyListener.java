/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

/**
 * Listener for changes in stream properties.
 */
public abstract class StreamPropertyListener {

  /**
   * Invoked when stream generation changed. Generation only increase monotonically, hence this method
   * is guaranteed to see only increasing generation across multiple calls.
   *
   * @param streamName Name of the stream
   * @param generation The generation id updated to.
   */
  public void generationChanged(String streamName, int generation) {
    // Default no-op
  }

  /**
   * Invoked when the stream generation property is deleted.
   *
   * @param streamName Name of the stream
   */
  public void generationDeleted(String streamName) {
    // Default no-op
  }

  /**
   * Invoked when the stream TTL property is changed.
   *
   * @param streamName Name of the stream
   * @param ttl TTL of the stream
   */
  public void ttlChanged(String streamName, long ttl) {
    // Default no-op
  }

  /**
   * Invoked when the stream TTL property is deleted.
   *
   * @param streamName Name of the stream
   */
  public void ttlDeleted(String streamName) {
    // Default no-op
  }
}
