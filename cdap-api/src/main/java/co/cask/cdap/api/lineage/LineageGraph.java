/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.api.lineage;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Graph representing set of {@link Input} from sources, set of {@link Output} from targets, and
 * set of {@link Operation} converting inputs to outputs.
 */
public class LineageGraph {
  private final Set<Input> sourceInputs;
  private final Set<Output> targetOutputs;
  private final Set<Operation> operations;

  private LineageGraph(Set<Input> sourceInputs, Set<Output> targetOutputs, Set<Operation> operations) {
    this.sourceInputs = Collections.unmodifiableSet(new HashSet<>(sourceInputs));
    this.targetOutputs = Collections.unmodifiableSet(new HashSet<>(targetOutputs));
    this.operations = Collections.unmodifiableSet(new HashSet<>(operations));
  }

  /**
   * @return the set of {@link Input} representing sources
   */
  public Set<Input> getSourceInputs() {
    return sourceInputs;
  }

  /**
   * @return the set of {@link Output} representing targets
   */
  public Set<Output> getTargetOutputs() {
    return targetOutputs;
  }

  /**
   * @return the set of {@link Operation} in the lineage graph
   */
  public Set<Operation> getOperations() {
    return operations;
  }

  /**
   * Builder for building {@link LineageGraph}.
   */
  public static class Builder {
    private final Set<Input> sourceInputs;
    private final Set<Output> targetOutputs;
    private final Set<Operation> operations;

    public Builder() {
      this.sourceInputs = new HashSet<>();
      this.targetOutputs = new HashSet<>();
      this.operations = new HashSet<>();
    }

    /**
     * Collect the {@link Input} representing source in the lineage graph.
     * @param sourceInputs {@link Input} representing sources
     * @return this instance of the Builder
     */
    public Builder fromSources(Collection<? extends Input> sourceInputs) {
      this.sourceInputs.addAll(sourceInputs);
      return this;
    }

    /**
     * Collect the {@link Output} representing the targets in the lineage graph.
     * @param targetOutputs {@link Output} representing the targets
     * @return this instance of the Builder
     */
    public Builder toTargets(Collection<? extends Output> targetOutputs) {
      this.targetOutputs.addAll(targetOutputs);
      return this;
    }

    /**
     * Add operation to the lineage graph.
     * @param operation operation representing transformation
     * @return this instance of the Builder
     */
    public Builder withOperation(Operation operation) {
      this.operations.add(operation);
      return this;
    }

    /**
     * Add collection of operations to the lineage graph.
     * @param operations collection of operations representing transformations
     * @return this instance of the Builder
     */
    public Builder withOperations(Collection<? extends Operation> operations) {
      this.operations.addAll(operations);
      return this;
    }

    /**
     * Build the LineageGraph.
     */
    public LineageGraph build() {
      return new LineageGraph(sourceInputs, targetOutputs, operations);
    }
  }
}
