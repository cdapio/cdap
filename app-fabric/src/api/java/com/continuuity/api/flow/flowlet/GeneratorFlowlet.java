package com.continuuity.api.flow.flowlet;

/**
 * A special kind of {@link Flowlet} that generates data and never consume any input.
 */
public interface GeneratorFlowlet extends Flowlet {

  /**
   * This method will be called by the system periodically. The implementation
   * should writes data through {@link OutputEmitter} in order to propagate
   * to downstream {@link Flowlet}.
   *
   * @throws Exception
   */
  void generate() throws Exception;
}
