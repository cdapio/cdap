/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.queue;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.google.common.base.Objects;
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
    private final FlowletConnection.Type type;
    private final String name;

    public static Node flowlet(String name) {
      return new Node(FlowletConnection.Type.FLOWLET, name);
    }

    public static Node stream(String name) {
      return new Node(FlowletConnection.Type.STREAM, name);
    }

    public Node(FlowletConnection.Type type, String name) {
      this.type = type;
      this.name = name;
    }

    public FlowletConnection.Type getType() {
      return type;
    }

    public String getName() {
      return name;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(type, name);
    }

    @Override
    public boolean equals(Object o) {
      if(o == null) {
        return false;
      }
      Node other = (Node) o;
      return Objects.equal(type, other.getType()) && Objects.equal(name, other.getName());
    }
  }

  /**
   * Given a {@link FlowSpecification} we generate a map of from flowlet
   * to to flowlet with their queue and schema.
   *
   * @param specification of a Flow
   * @return A {@link Table} consisting of From, To Flowlet and QueueSpecification.
   */
  Table<Node, String, Set<QueueSpecification>> create(FlowSpecification specification);
}
