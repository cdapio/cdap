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

import co.cask.cdap.etl.proto.v2.spec.StageSpec;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Response for validating a pipeline stage.
 */
public class StageValidationResponse {
  private final List<? extends ValidationError> errors;
  private final StageSpec spec;

  public StageValidationResponse(List<? extends ValidationError> errors) {
    this(errors, null);
  }

  public StageValidationResponse(StageSpec spec) {
    this(Collections.emptyList(), spec);
  }

  private StageValidationResponse(List<? extends ValidationError> errors, @Nullable StageSpec spec) {
    this.errors = errors;
    this.spec = spec;
  }

  public List<? extends ValidationError> getErrors() {
    return errors == null ? Collections.emptyList() : errors;
  }

  @Nullable
  public StageSpec getSpec() {
    return spec;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StageValidationResponse that = (StageValidationResponse) o;
    return Objects.equals(errors, that.errors) &&
      Objects.equals(spec, that.spec);
  }

  @Override
  public int hashCode() {
    return Objects.hash(errors, spec);
  }
}
