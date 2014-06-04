/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import java.io.IOException;

/**
 * Factory for creating {@link StreamConsumerStateStore} instance for different streams.
 */
public interface StreamConsumerStateStoreFactory {

  /**
   * Creates a {@link StreamConsumerStateStore} for the given stream.
   *
   * @param streamConfig Configuration of the stream.
   * @return a new state store instance.
   */
  StreamConsumerStateStore create(StreamConfig streamConfig) throws IOException;

  /**
   * Deletes all consumer state stores.
   */
  void dropAll() throws IOException;
}
