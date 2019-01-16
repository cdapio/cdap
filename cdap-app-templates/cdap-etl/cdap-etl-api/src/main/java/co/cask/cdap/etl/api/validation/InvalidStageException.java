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

package co.cask.cdap.etl.api.validation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Thrown when a pipeline stage is invalid. If there are multiple reasons the stage is invalid, they can
 * all be specified together in this exception. This allows end users to see all errors at once instead of one by one.
 */
public class InvalidStageException extends RuntimeException {
  private final List<Throwable> reasons;

  public InvalidStageException(String message) {
    super(message);
    this.reasons = Collections.emptyList();
  }

  public InvalidStageException(String message, Throwable cause) {
    super(message, cause);
    this.reasons = Collections.singletonList(cause);
  }

  public InvalidStageException(Collection<Throwable> reasons) {
    this.reasons = Collections.unmodifiableList(new ArrayList<>(reasons));
  }

  public List<Throwable> getReasons() {
    return reasons;
  }
}
