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
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a single operation happening on the set of {@link Input}s to create set of {@link Output}s.
 */
public class FieldOperation {
  private final String name;
  private final String description;
  private final Set<Input> inputs;
  private final Set<Output> outputs;

  private FieldOperation(String name, String description, Set<Input> inputs, Set<Output> outputs) {
    this.name = name;
    this.description = description;
    this.inputs = inputs;
    this.outputs = outputs;
  }

  /**
   * Builder for creating FieldOperation
   */
  public static class Builder {
    private final String name;
    private String description;
    private Set<Input> inputs;
    private Set<Output> outputs;

    public Builder(String name) {
      this.name = name;
      this.description = "";
      this.inputs = new HashSet<>();
      this.outputs = new HashSet<>();
    }

    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    public Builder addInput(Input input) {
      this.inputs.add(input);
      return this;
    }

    public Builder addInputs(Collection<Input> inputs) {
      this.inputs.addAll(inputs);
      return this;
    }

    public Builder addOutput(Output output) {
      this.outputs.add(output);
      return this;
    }

    public Builder addOutputs(Collection<Output> outputs) {
      this.outputs.addAll(outputs);
      return this;
    }

    public FieldOperation build() {
      return new FieldOperation(name, description, inputs, outputs);
    }
  }
}

