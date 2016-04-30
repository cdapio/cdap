/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.spec;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.proto.v2.ETLStage;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Specification for a pipeline stage.
 *
 * This is like an {@link ETLStage}, but has additional attributes calculated at configure time of the application.
 * The spec contains the input and output schema (if known) for the stage, as well as any output stages it writes to.
 *
 * TODO: add other useful information, like the datasets, streams, and other plugins used by this stage.
 */
public class StageSpec {
  private final String name;
  private final PluginSpec plugin;
  private final String errorDatasetName;
  private final Schema inputSchema;
  private final Schema outputSchema;
  private final Set<String> inputs;
  private final Set<String> outputs;

  private StageSpec(String name, PluginSpec plugin, String errorDatasetName,
                    Schema inputSchema, Schema outputSchema, Set<String> inputs, Set<String> outputs) {
    this.name = name;
    this.plugin = plugin;
    this.errorDatasetName = errorDatasetName;
    this.inputSchema = inputSchema;
    this.outputSchema = outputSchema;
    this.inputs = ImmutableSet.copyOf(inputs);
    this.outputs = ImmutableSet.copyOf(outputs);
  }

  public String getName() {
    return name;
  }

  public PluginSpec getPlugin() {
    return plugin;
  }

  public String getErrorDatasetName() {
    return errorDatasetName;
  }

  public Schema getInputSchema() {
    return inputSchema;
  }

  public Schema getOutputSchema() {
    return outputSchema;
  }

  public Set<String> getInputs() {
    return inputs;
  }

  public Set<String> getOutputs() {
    return outputs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StageSpec that = (StageSpec) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(plugin, that.plugin) &&
      Objects.equals(errorDatasetName, that.errorDatasetName) &&
      Objects.equals(inputSchema, that.inputSchema) &&
      Objects.equals(outputSchema, that.outputSchema) &&
      Objects.equals(inputs, that.inputs) &&
      Objects.equals(outputs, that.outputs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, plugin, errorDatasetName, inputSchema, outputSchema, inputs, outputs);
  }

  @Override
  public String toString() {
    return "StageSpec{" +
      "name='" + name + '\'' +
      ", plugin=" + plugin +
      ", errorDatasetName='" + errorDatasetName + '\'' +
      ", inputSchema=" + inputSchema +
      ", outputSchema=" + outputSchema +
      ", inputs=" + inputs +
      ", outputs=" + outputs +
      '}';
  }

  public static Builder builder(String name, PluginSpec plugin) {
    return new Builder(name, plugin);
  }

  /**
   * Builder for a StageSpec.
   */
  public static class Builder {
    private final String name;
    private final PluginSpec plugin;
    private String errorDatasetName;
    private Schema inputSchema;
    private Schema outputSchema;
    private Set<String> inputs;
    private Set<String> outputs;

    public Builder(String name, PluginSpec plugin) {
      this.name = name;
      this.plugin = plugin;
      this.inputs = new HashSet<>();
      this.outputs = new HashSet<>();
    }

    public Builder setErrorDatasetName(String errorDatasetName) {
      this.errorDatasetName = errorDatasetName;
      return this;
    }

    public Builder setInputSchema(Schema inputSchema) {
      this.inputSchema = inputSchema;
      return this;
    }

    public Builder setOutputSchema(Schema outputSchema) {
      this.outputSchema = outputSchema;
      return this;
    }

    public Builder addInputs(Collection<String> inputs) {
      this.inputs.addAll(inputs);
      return this;
    }

    public Builder addInputs(String... inputs) {
      for (String input : inputs) {
        this.inputs.add(input);
      }
      return this;
    }

    public Builder addOutputs(Collection<String> outputs) {
      this.outputs.addAll(outputs);
      return this;
    }

    public Builder addOutputs(String... outputs) {
      for (String output : outputs) {
        this.outputs.add(output);
      }
      return this;
    }

    public StageSpec build() {
      return new StageSpec(name, plugin, errorDatasetName, inputSchema, outputSchema, inputs, outputs);
    }

  }
}
