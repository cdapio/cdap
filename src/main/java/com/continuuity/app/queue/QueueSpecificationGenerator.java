/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.queue;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.internal.app.queue.SimpleQueueSpecificationGenerator;
import com.google.common.collect.Table;

import java.util.Set;

/**
 * This interface specifies how the {@link QueueSpecification} is generated
 * for each {@link com.continuuity.api.flow.flowlet.Flowlet} and within
 * each Flowlet the inputs and outputs.
 * <p/>
 * <p>
 * This requires looking at the whole Specification..
 * </p>
 */
public interface QueueSpecificationGenerator {

  /**
   * This class represents a node in the DAG.
   */
  public static final class Node {
    private final FlowletConnection.SourceType type;
    private final String source;

    public Node(FlowletConnection.SourceType type, String source) {
      this.type = type;
      this.source = source;
    }

    public FlowletConnection.SourceType getSourceType() {
      return type;
    }

    public String getSourceName() {
      return source;
    }
  }

  /**
   * Given a {@link FlowSpecification} we generate a map of from flowlet
   * to to flowlet with their queue and schema.
   *
   * @param specification of a Flow
   * @return A {@link Table} consisting of From, To Flowlet and QueueSpecification.
   */
  Table<String, String, Set<QueueSpecification>> create(FlowSpecification specification);
}
