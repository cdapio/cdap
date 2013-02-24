/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.pipeline;

/**
 * This interface represents the context in which a stage is running.
 * Ordinarily the context will be provided by the pipeline in which
 * the stage is embedded.
 */
public interface Context {
  /**
   * Used when you a {@link Stage} wants to send data to the downstream stage
   *
   * @param o to be send to next stage.
   */
  void setDownStream(Object o);

  /**
   * @return Object passed through from the upstream
   */
  Object getUpStream();

  /**
   * @return Object being send to downstream
   */
  Object getDownStream();
}
