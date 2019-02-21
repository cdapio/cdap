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
 */

package co.cask.cdap.k8s.program;

import co.cask.cdap.proto.id.ProgramRunId;

/**
 * Utilities for Kubernetes programs.
 */
public class KubernetesPrograms {

  private KubernetesPrograms() {
    // no-op
  }

  /**
   * @return name of any resource that needs to be created for a program run.
   */
  public static String getResourceName(ProgramRunId programRunId) {
    return String.format("%s-%s", programRunId.getProgram().toLowerCase(), programRunId.getRun());
  }
}
