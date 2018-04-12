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

import co.cask.cdap.proto.id.ProgramRunId;

/**
 * A Provisioning related operation.
 */
public abstract class ProvisioningTask implements Runnable {
  protected final ProgramRunId programRunId;

  public ProvisioningTask(ProgramRunId programRunId) {
    this.programRunId = programRunId;
  }

  /**
   * @return the program run this is for.
   */
  public ProgramRunId getProgramRunId() {
    return programRunId;
  }
}
