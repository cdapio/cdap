/*
 * Copyright © 2014-2022 Cask Data, Inc.
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
import io.cdap.cdap.proto.id.ProgramReference;

/**
 * Thrown when a program is not found
 */
public class ProgramNotFoundException extends NotFoundException {

  private final ProgramReference ref;

  public ProgramNotFoundException(ProgramId programId) {
    super(programId);
    this.ref = programId.getProgramReference();
  }

  public ProgramNotFoundException(ProgramReference programRef) {
    super(programRef);
    this.ref = programRef;
  }

  public ProgramReference getId() {
    return ref;
  }
}
