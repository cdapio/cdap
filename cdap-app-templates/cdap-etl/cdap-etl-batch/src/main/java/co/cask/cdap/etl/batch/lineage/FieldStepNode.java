/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.batch.lineage;

import co.cask.cdap.proto.id.ProgramRunId;

import java.util.Objects;

/**
 * A FieldStepNode represents one transformation step applied to a field of data
 */
public class FieldStepNode implements FieldLevelLineageStoreNode {
  private final ProgramRunId pipeline;
  private final String stage;
  private final String field;
  private final int stepNumber;

  public FieldStepNode(ProgramRunId pipeline, String stage, String field, int stepNumber) {
    this.pipeline = pipeline;
    this.stage = stage;
    this.field = field;
    this.stepNumber = stepNumber;
  }

  @Override
  public ProgramRunId getPipeline() {
    return this.pipeline;
  }

  @Override
  public String getStage() {
    return this.stage;
  }

  @Override
  public String getField() {
    return this.field;
  }

  public int getStepNumber() {
    return this.stepNumber;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FieldStepNode that = (FieldStepNode) o;
    return Objects.equals(this.pipeline, that.pipeline) && Objects.equals(this.stage, that.stage) &&
      Objects.equals(this.field, that.field) && Objects.equals(this.stepNumber, that.stepNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeline, stage, field, stepNumber);
  }

  @Override
  public String toString() {
    return "(Pipeline: " + pipeline + ", Stage Name: " + stage + ", Field: " + field + ", Step#: " + stepNumber + ")";
  }
}
