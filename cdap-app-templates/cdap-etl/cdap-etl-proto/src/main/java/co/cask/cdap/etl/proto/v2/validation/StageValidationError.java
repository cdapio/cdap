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

import java.util.Objects;

/**
 * An error specific to a single pipeline stage.
 */
public class StageValidationError extends ValidationError {
  protected final String stage;

  public StageValidationError(String message, String stage) {
    super(Type.STAGE_ERROR, message);
    this.stage = stage;
  }

  protected StageValidationError(Type type, String message, String stage) {
    super(type, message);
    this.stage = stage;
  }

  public String getStage() {
    return stage;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    StageValidationError that = (StageValidationError) o;
    return Objects.equals(stage, that.stage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), stage);
  }
}
