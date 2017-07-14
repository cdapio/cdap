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
import co.cask.cdap.etl.api.SplitterTransform;
import co.cask.cdap.etl.proto.v2.ETLStage;
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Specification for a pipeline stage.
 *
 * This is like an {@link ETLStage}, but has additional attributes calculated at configure time of the application.
 * The spec contains the input and output schema (if known) for the stage, as well as any output stages it writes to.
 */
public class StageSpec implements Serializable {
  private static final long serialVersionUID = 8178852817751037188L;
  private final String name;
  private final PluginSpec plugin;
  private final String errorDatasetName;
  private final Map<String, Schema> inputSchemas;
  private final Map<String, Port> outputPorts;
  private final Schema outputSchema;
  private final Schema errorSchema;
  private final boolean stageLoggingEnabled;
  private final boolean processTimingEnabled;
  // here for backwards compatible with UI
  private final Set<String> inputs;
  private final Set<String> outputs;

  private StageSpec(String name, PluginSpec plugin, String errorDatasetName, Map<String, Schema> inputSchemas,
                    Map<String, Port> outputPorts, Schema errorSchema,
                    boolean stageLoggingEnabled, boolean processTimingEnabled) {
    this.name = name;
    this.plugin = plugin;
    this.errorDatasetName = errorDatasetName;
    this.inputSchemas = Collections.unmodifiableMap(inputSchemas);
    this.outputPorts = Collections.unmodifiableMap(outputPorts);
    this.outputSchema = plugin.getType().equals(SplitterTransform.PLUGIN_TYPE) || outputPorts.isEmpty() ?
      null : outputPorts.values().iterator().next().schema;
    this.errorSchema = errorSchema;
    this.inputs = ImmutableSet.copyOf(inputSchemas.keySet());
    this.outputs = ImmutableSet.copyOf(outputPorts.keySet());
    this.stageLoggingEnabled = stageLoggingEnabled;
    this.processTimingEnabled = processTimingEnabled;
  }

  public String getName() {
    return name;
  }

  public PluginSpec getPlugin() {
    return plugin;
  }

  public String getPluginType() {
    return plugin.getType();
  }

  public String getErrorDatasetName() {
    return errorDatasetName;
  }

  public Map<String, Schema> getInputSchemas() {
    return inputSchemas;
  }

  public Schema getOutputSchema() {
    return outputSchema;
  }

  public Map<String, Port> getOutputPorts() {
    return outputPorts;
  }

  public Schema getErrorSchema() {
    return errorSchema;
  }

  public Set<String> getInputs() {
    return inputs;
  }

  public Set<String> getOutputs() {
    return outputs;
  }

  public boolean isStageLoggingEnabled() {
    return stageLoggingEnabled;
  }

  public boolean isProcessTimingEnabled() {
    return processTimingEnabled;
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
      Objects.equals(inputSchemas, that.inputSchemas) &&
      Objects.equals(outputPorts, that.outputPorts) &&
      Objects.equals(outputSchema, that.outputSchema) &&
      Objects.equals(errorSchema, that.errorSchema) &&
      Objects.equals(inputs, that.inputs) &&
      Objects.equals(outputs, that.outputs) &&
      stageLoggingEnabled == that.stageLoggingEnabled &&
      processTimingEnabled == that.processTimingEnabled;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, plugin, errorDatasetName, inputSchemas, outputPorts,
                        outputSchema, errorSchema, inputs, outputs, stageLoggingEnabled, processTimingEnabled);
  }

  @Override
  public String toString() {
    return "StageSpec{" +
      "name='" + name + '\'' +
      ", plugin=" + plugin +
      ", errorDatasetName='" + errorDatasetName + '\'' +
      ", inputSchemas=" + inputSchemas +
      ", outputPorts=" + outputPorts +
      ", outputSchema=" + outputSchema +
      ", errorSchema=" + errorSchema +
      ", stageLoggingEnabled=" + stageLoggingEnabled +
      ", processTimingEnabled=" + processTimingEnabled +
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
    private Map<String, Schema> inputSchemas;
    private Map<String, Port> outputPortSchemas;
    private Schema errorSchema;
    private boolean stageLoggingEnabled;
    private boolean processTimingEnabled;

    public Builder(String name, PluginSpec plugin) {
      this.name = name;
      this.plugin = plugin;
      this.inputSchemas = new HashMap<>();
      this.outputPortSchemas = new HashMap<>();
      this.stageLoggingEnabled = true;
      this.processTimingEnabled = true;
    }

    public Builder setErrorDatasetName(String errorDatasetName) {
      this.errorDatasetName = errorDatasetName;
      return this;
    }

    public Builder addInputSchema(String stageName, Schema schema) {
      this.inputSchemas.put(stageName, schema);
      return this;
    }

    public Builder addInputSchemas(Map<String, Schema> inputSchemas) {
      this.inputSchemas.putAll(inputSchemas);
      return this;
    }

    public Builder addOutputSchema(@Nullable Schema outputSchema, String... stages) {
      for (String stage : stages) {
        outputPortSchemas.put(stage, new Port(null, outputSchema));
      }
      return this;
    }

    public Builder addOutputPortSchema(String outputStageName, String port, @Nullable Schema outputSchema) {
      this.outputPortSchemas.put(outputStageName, new Port(port, outputSchema));
      return this;
    }

    public Builder addOutputPortSchemas(Map<String, Port> outputStages) {
      this.outputPortSchemas.putAll(outputStages);
      return this;
    }

    public Builder setErrorSchema(Schema errorSchema) {
      this.errorSchema = errorSchema;
      return this;
    }

    public Builder setStageLoggingEnabled(boolean stageLoggingEnabled) {
      this.stageLoggingEnabled = stageLoggingEnabled;
      return this;
    }

    public Builder setProcessTimingEnabled(boolean processTimingEnabled) {
      this.processTimingEnabled = processTimingEnabled;
      return this;
    }

    public StageSpec build() {
      return new StageSpec(name, plugin, errorDatasetName, inputSchemas, outputPortSchemas, errorSchema,
                           stageLoggingEnabled, processTimingEnabled);
    }

  }

  /**
   * Represents an output port.
   */
  public static class Port implements Serializable {
    private static final long serialVersionUID = -8265114217209734806L;
    private final String port;
    private final Schema schema;

    public Port(@Nullable String port, @Nullable Schema schema) {
      this.port = port;
      this.schema = schema;
    }

    /**
     * @return the output port that the stage is connected to. A null port means all output is sent to the stage
     */
    @Nullable
    public String getPort() {
      return port;
    }

    /**
     * @return the schema of the output port. A null schema means it is variable or unknown
     */
    @Nullable
    public Schema getSchema() {
      return schema;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Port that = (Port) o;

      return Objects.equals(port, that.port) && Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(port, schema);
    }
  }
}
