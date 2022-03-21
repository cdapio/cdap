/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common;

import io.cdap.cdap.proto.id.ProgramId;

/**
 * Exception to be thrown when program runs are forbidden on certain environments.
 */
public class ProgramRunForbiddenException extends ForbiddenException {

  public ProgramRunForbiddenException(ProgramId programId) {
    super(String.format("Program %s can not be run because launching " +
                          "user programs with native profile is disabled.", programId.toString()));
  }
}
