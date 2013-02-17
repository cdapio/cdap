/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app;

import com.continuuity.api.flow.FlowSpecification;
import com.google.common.collect.Table;

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
   * Given a {@link FlowSpecification} we generate a map of from flowlet
   * to to flowlet with their queue and schema.
   *
   * @param specification of a Flow
   * @return A {@link Table} consisting of From, To Flowlet and QueueSpecification.
   */
  Table<String, String, QueueSpecification> create(FlowSpecification specification);
}
