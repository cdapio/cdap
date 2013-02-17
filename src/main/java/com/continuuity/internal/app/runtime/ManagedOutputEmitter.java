package com.continuuity.internal.app.runtime;

import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.util.List;

/**
 *
 */
public interface ManagedOutputEmitter<T> extends OutputEmitter<T> {

  /**
   * Returns all the emitted datum. This also stop accepting new datum being emitted to this emitter until
   * {@link #reset()} is called.
   *
   * @return An immutable list of {@link EmittedDatum} that has been written to this emitter since last reset.
   */
  List<EmittedDatum> capture();

  /**
   * Reset this emitter to allow new datum being written again.
   */
  void reset();
}
