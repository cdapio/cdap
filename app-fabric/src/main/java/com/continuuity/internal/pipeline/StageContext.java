/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.pipeline;

import com.continuuity.pipeline.Context;

/**
 * Concrete implementation of {@link Context} for moving data from downstream
 * and to upstream stages.
 */
public final class StageContext implements Context {
  private Object upStream;
  private Object downStream;

  /**
   * Constructor constructed when the result is available from upstream.
   *
   * @param upStream Object data
   */
  public StageContext(Object upStream) {
    this.upStream = upStream;
  }

  /**
   * Sets result to be sent to downstream from the current stage.
   *
   * @param downStream Object to be sent to downstream
   */
  @Override
  public void setDownStream(Object downStream) {
    this.downStream = downStream;
  }

  /**
   * @return Object received from upstream
   */
  @Override
  public Object getUpStream() {
    return upStream;
  }

  /**
   * @return Object to be sent to downstream.
   */
  @Override
  public Object getDownStream() {
    return downStream;
  }
}
