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

package co.cask.cdap.internal.provision;

import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.proto.id.ProgramRunId;

/**
 * Information needed to provision a cluster.
 */
public class ProvisionRequest {
  private final ProgramRunId programRunId;
  private final ProgramOptions programOptions;
  private final ProgramDescriptor programDescriptor;
  private final String user;

  public ProvisionRequest(ProgramRunId programRunId, ProgramOptions programOptions,
                          ProgramDescriptor programDescriptor, String user) {
    this.programRunId = programRunId;
    this.programOptions = programOptions;
    this.programDescriptor = programDescriptor;
    this.user = user;
  }

  public ProgramRunId getProgramRunId() {
    return programRunId;
  }

  public ProgramOptions getProgramOptions() {
    return programOptions;
  }

  public ProgramDescriptor getProgramDescriptor() {
    return programDescriptor;
  }

  public String getUser() {
    return user;
  }
}
