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

  void skip();
}
