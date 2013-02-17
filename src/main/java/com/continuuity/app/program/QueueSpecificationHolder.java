/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

/**
 * This class is holder for each flowlet with a map for it's input/outputs
 * from input or output of a flowlet to their specifications and schema.
 */
public final class QueueSpecificationHolder {
  private final Map<String, Set<QueueSpecification>> inputs;
  private final Map<String, Set<QueueSpecification>> outputs;

  public QueueSpecificationHolder(Map<String, Set<QueueSpecification>> inputs,
                                  Map<String, Set<QueueSpecification>> outputs) {
    this.inputs = inputs;
    this.outputs = outputs;
  }

  /**
   * @return Mapping of input and their schema
   */
  public Map<String, Set<QueueSpecification>> getInputs() {
    return inputs;
  }

  /**
   * @return Mapping of output and their schema.
   */
  public Map<String, Set<QueueSpecification>> getOutputs() {
    return outputs;
  }
}
