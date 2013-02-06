package com.continuuity.api.flow.flowlet;

/**
 * Represents the context of the output data that flowlet is using
 * forward to next flowlet.
 */
public interface OutputContext {

  /**
   * @return Name of the flowlet.
   */
  String getName();
}
