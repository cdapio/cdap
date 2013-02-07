/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

/**
 * This interface defines an emitter used for emitting events from
 * within a flowlet.
 */
public interface OutputEmitter<T> {
  /**
   * Emits a event of type T
   * @param data to be emitted by the emitter which is of type T
   */
  void emit(T data);
}
