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

import com.google.common.base.Objects;

/**
 * Represents an aggregate of program runs by the user who starts the program run.
 */
public class UserAggregate extends ProgramRunAggregate {
  private final String user;

  public UserAggregate(String user, long runs) {
    super(runs);
    this.user = user;
  }

  /**
   * @return the name of the user who started the program run
   */
  public String getUser() {
    return user;
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

    UserAggregate that = (UserAggregate) o;
    return Objects.equal(user, that.user);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), user);
  }
}
