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

import co.cask.cdap.etl.proto.v2.ETLStage;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * Class to hold a stage with its input and outputs.
 */
public class StageConnections {
  private final ETLStage stage;
  private final Set<String> inputs;
  private final Set<String> outputs;

  public StageConnections(ETLStage stage, Collection<String> inputs, Collection<String> outputs) {
    this.stage = stage;
    this.inputs = ImmutableSet.copyOf(inputs);
    this.outputs = ImmutableSet.copyOf(outputs);
  }


  public ETLStage getStage() {
    return stage;
  }

  public Set<String> getOutputs() {
    return outputs;
  }

  public Set<String> getInputs() {
    return inputs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StageConnections that = (StageConnections) o;

    return Objects.equals(stage, that.stage) &&
      Objects.equals(outputs, that.outputs) &&
      Objects.equals(inputs, that.inputs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stage, outputs, inputs);
  }

  @Override
  public String toString() {
    return "StageConnections{" +
      "stage=" + stage +
      ", outputs=" + outputs +
      ", inputs=" + inputs +
      '}';
  }
}
