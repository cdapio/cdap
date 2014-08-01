/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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
      if (o == null) {
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
