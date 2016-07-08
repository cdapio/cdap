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

package co.cask.cdap.etl.planner;

import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Class to encapsulate information needed about a plugin at runtime.
 */
public class StageInfo {
  private final String name;
  private final String pluginType;
  private final Set<String> inputs;
  private final Map<String, Schema> inputSchemas;
  private final Set<String> outputs;
  private final Schema outputSchema;
  private final String errorDatasetName;

  private StageInfo(String name, String pluginType, Set<String> inputs, Map<String, Schema> inputSchemas,
                    Set<String> outputs, @Nullable Schema outputSchema, @Nullable String errorDatasetName) {
    this.name = name;
    this.pluginType = pluginType;
    this.inputSchemas = Collections.unmodifiableMap(inputSchemas);
    this.outputSchema = outputSchema;
    this.inputs = ImmutableSet.copyOf(inputs);
    this.outputs = ImmutableSet.copyOf(outputs);
    this.errorDatasetName = errorDatasetName;
  }

  public String getName() {
    return name;
  }

  public String getPluginType() {
    return pluginType;
  }

  public Set<String> getInputs() {
    return inputs;
  }

  public Map<String, Schema> getInputSchemas() {
    return inputSchemas;
  }

  public Set<String> getOutputs() {
    return outputs;
  }

  @Nullable
  public Schema getOutputSchema() {
    return outputSchema;
  }

  @Nullable
  public String getErrorDatasetName() {
    return errorDatasetName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StageInfo that = (StageInfo) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(pluginType, that.pluginType) &&
      Objects.equals(inputs, that.inputs) &&
      Objects.equals(inputSchemas, that.inputSchemas) &&
      Objects.equals(outputs, that.outputs) &&
      Objects.equals(outputSchema, that.outputSchema) &&
      Objects.equals(errorDatasetName, that.errorDatasetName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, pluginType, inputs, inputSchemas,
                        outputs, outputSchema, errorDatasetName);
  }

  @Override
  public String toString() {
    return "StageInfo{" +
      "name='" + name + '\'' +
      "pluginType='" + pluginType + '\'' +
      "inputs='" + inputs + '\'' +
      "inputSchemas='" + inputSchemas + '\'' +
      "outputs='" + outputs + '\'' +
      "outputSchema='" + outputSchema + '\'' +
      ", errorDatasetName='" + errorDatasetName + '\'' +
      '}';
  }

  public static Builder builder(String name, String pluginType) {
    return new Builder(name, pluginType);
  }

  /**
   * Builder to create StageInfo.
   */
  public static class Builder {
    private final String name;
    private final String pluginType;
    private final Set<String> inputs;
    private final Set<String> outputs;
    private final Map<String, Schema> inputSchemas;
    private Schema outputSchema;
    private String errorDatasetName;

    public Builder(String name, String pluginType) {
      this.name = name;
      this.pluginType = pluginType;
      this.inputs = new HashSet<>();
      this.outputs = new HashSet<>();
      this.inputSchemas = new HashMap<>();
    }

    public Builder addInputs(String... inputs) {
      Collections.addAll(this.inputs, inputs);
      return this;
    }

    public Builder addInputs(Collection<String> inputs) {
      this.inputs.addAll(inputs);
      return this;
    }

    public Builder addOutputs(String... outputs) {
      Collections.addAll(this.outputs, outputs);
      return this;
    }

    public Builder addOutputs(Collection<String> outputs) {
      this.outputs.addAll(outputs);
      return this;
    }

    public Builder addInputSchema(String inputStageName, Schema schema) {
      inputSchemas.put(inputStageName, schema);
      return this;
    }

    public Builder addInputSchemas(Map<String, Schema> schemas) {
      inputSchemas.putAll(schemas);
      return this;
    }

    public Builder setOutputSchema(Schema schema) {
      outputSchema = schema;
      return this;
    }

    public Builder setErrorDatasetName(String errorDatasetName) {
      this.errorDatasetName = errorDatasetName;
      return this;
    }

    public StageInfo build() {
      return new StageInfo(name, pluginType, inputs, inputSchemas, outputs, outputSchema, errorDatasetName);
    }
  }
}
