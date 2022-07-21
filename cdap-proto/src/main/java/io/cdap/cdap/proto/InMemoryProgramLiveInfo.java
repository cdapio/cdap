/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.proto;

import io.cdap.cdap.proto.id.ProgramId;

import java.util.List;

/**
 * A live info for in-memory runtime environment.
 */
public class InMemoryProgramLiveInfo extends ProgramLiveInfo {
  private final List<String> services;

  public InMemoryProgramLiveInfo(ProgramId programId) {
    this(programId, null);
  }

  public InMemoryProgramLiveInfo(ProgramId programId, List<String> services) {
    super(programId, "in-memory");
    this.services = services;
  }
}
