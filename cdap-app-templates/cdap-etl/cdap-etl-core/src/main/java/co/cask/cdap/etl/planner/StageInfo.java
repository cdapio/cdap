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

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Class to encapsulate information needed about a plugin at runtime.
 */
public class StageInfo {
  private final String name;
  private final Map<String, Schema> inputSchemas;
  private final Schema outputSchema;
  private final Set<String> inputs;
  private final Set<String> outputs;
  private final String errorDatasetName;

  public StageInfo(String name) {
    this(name, null);
  }

  public StageInfo(String name, @Nullable String errorDatasetName) {
    this.name = name;
    this.errorDatasetName = errorDatasetName;
    this.inputSchemas = null;
    this.outputSchema = null;
    this.inputs = null;
    this.outputs = null;
  }

  public StageInfo(String name, Map<String, Schema> inputSchemas, Schema outputSchema,
                   Set<String> inputs, Set<String> outputs, String errorDatasetName) {
    this.name = name;
    this.inputSchemas = inputSchemas;
    this.outputSchema = outputSchema;
    this.inputs = inputs;
    this.outputs = outputs;
    this.errorDatasetName = errorDatasetName;
  }

  public Map<String, Schema> getInputSchemas() {
    return inputSchemas;
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

  public String getName() {
    return name;
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
      Objects.equals(errorDatasetName, that.errorDatasetName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, errorDatasetName);
  }

  @Override
  public String toString() {
    return "StageInfo{" +
      "name='" + name + '\'' +
      ", errorDatasetName='" + errorDatasetName + '\'' +
      '}';
  }
}
