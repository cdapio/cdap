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

/**
 * Represents information about running programs. This class can be extended to add information for
 * specific runtime environments.
 */
public abstract class ProgramLiveInfo {

  private final String app;
  private final String type;
  private final String name;
  private final String runtime;

  public ProgramLiveInfo(ProgramId programId, String runtime) {
    this.app = programId.getApplication();
    this.type = programId.getType().getPrettyName();
    this.name = programId.getProgram();
    this.runtime = runtime;
  }

  public String getApp() {
    return app;
  }

  public ProgramType getType() {
    return ProgramType.valueOfPrettyName(type);
  }

  public String getName() {
    return name;
  }

  public String getRuntime() {
    return runtime;
  }
}
