/*
 * Copyright © 2019 Cask Data, Inc.
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
 *
 */

package co.cask.cdap.etl.proto.v2.validation;

import co.cask.cdap.etl.proto.v2.spec.PipelineSpec;

import java.util.List;
import java.util.Objects;

/**
 * Response for validating a pipeline.
 */
public class PipelineValidationResponse {
  private final List<? extends ValidationError> errors;
  private final PipelineSpec spec;

  public PipelineValidationResponse(List<? extends ValidationError> errors,
                                    PipelineSpec spec) {
    this.errors = errors;
    this.spec = spec;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipelineValidationResponse that = (PipelineValidationResponse) o;
    return Objects.equals(errors, that.errors) &&
      Objects.equals(spec, that.spec);
  }

  @Override
  public int hashCode() {
    return Objects.hash(errors, spec);
  }
}
