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
 */

package co.cask.cdap.report.proto.summary;

import co.cask.cdap.report.proto.ProgramRunStartMethod;
import com.google.common.base.Objects;

/**
 * Represents an aggregate of program runs by the start method.
 */
public class StartMethodAggregate extends ProgramRunAggregate {
  private final ProgramRunStartMethod method;

  public StartMethodAggregate(ProgramRunStartMethod method, long runs) {
    super(runs);
    this.method = method;
  }

  public StartMethodAggregate(String method, long runs) {
    super(runs);
    this.method = ProgramRunStartMethod.valueOf(method);
  }

  /**
   * @return the method of how the program run was started
   */
  public ProgramRunStartMethod getMethod() {
    return method;
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

    StartMethodAggregate that = (StartMethodAggregate) o;
    return Objects.equal(method, that.method);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), method);
  }
}
