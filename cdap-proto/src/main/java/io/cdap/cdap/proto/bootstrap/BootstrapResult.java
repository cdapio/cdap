/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.proto.bootstrap;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Result of executing bootstrap steps.
 */
public class BootstrapResult {
  private final List<BootstrapStepResult> steps;

  public BootstrapResult(List<BootstrapStepResult> steps) {
    this.steps = steps;
  }

  public List<BootstrapStepResult> getSteps() {
    return Collections.unmodifiableList(steps);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BootstrapResult that = (BootstrapResult) o;
    return Objects.equals(steps, that.steps);
  }

  @Override
  public int hashCode() {
    return Objects.hash(steps);
  }

  @Override
  public String toString() {
    return "BootstrapResult{" +
      "steps=" + steps +
      '}';
  }
}
