/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import com.continuuity.api.flow.FlowSpecification;

import java.util.Map;

/**
 * This interface specifies how the {@link QueueSpecification} is generated
 * for each {@link com.continuuity.api.flow.flowlet.Flowlet} and within
 * each Flowlet the inputs and outputs.
 *
 * <p>
 *   This requires looking at the whole Specification..
 * </p>
 */
public interface QueueSpecificationGenerator {

  /**
   * Given a {@link FlowSpecification} it will return a map of flowlet to
   * the {@link QueueSpecificationHolder}
   *
   * @param app of the application
   * @param specification of a Flow
   * @return Map of flowlet names to the {@link QueueSpecificationHolder}
   */
  Map<String, QueueSpecificationHolder> create(String account, String app, FlowSpecification specification);
}
