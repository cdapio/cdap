package com.continuuity.app.queue;

import com.continuuity.api.flow.flowlet.InputContext;

import java.nio.ByteBuffer;

/**
 *
 */
public interface InputDatum extends Iterable<ByteBuffer> {

  boolean needProcess();

  void incrementRetry();

  int getRetry();

  InputContext getInputContext();

  /**
   * Reclaim the input from the queue consumer. It is needed for processing retried entries.
   */
  void reclaim();

  /**
   * Returns number of entries in this Iterable.
   */
  int size();
}
