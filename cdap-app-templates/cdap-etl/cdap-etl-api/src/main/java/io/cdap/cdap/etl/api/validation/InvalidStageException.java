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

package io.cdap.cdap.etl.api.validation;

import java.util.ArrayList;
import java.util.List;

/**
 * Thrown when a pipeline stage is invalid for any reason. If there are multiple reasons that the stage is invalid,
 * they should all be specified.
 *
 * Deprecated since 6.1.0. Use {@link ValidationFailure} instead.
 */
@Deprecated
public class InvalidStageException extends RuntimeException {
  private final List<? extends InvalidStageException> reasons;

  /**
   * Used when there is a single reason that the pipeline stage is invalid.
   *
   * @param message message indicating what is invalid
   */
  public InvalidStageException(String message) {
    super(message);
    reasons = new ArrayList<>();
  }

  public InvalidStageException(String s, Throwable throwable) {
    super(s, throwable);
    this.reasons = new ArrayList<>();
  }

  /**
   * Used when there are multiple reasons that the pipeline stage is invalid.
   *
   * @param reasons the reasons that the stage is invalid
   */
  public InvalidStageException(List<? extends InvalidStageException> reasons) {
    this.reasons = new ArrayList<>(reasons);
  }

  public List<? extends InvalidStageException> getReasons() {
    return reasons;
  }
}
