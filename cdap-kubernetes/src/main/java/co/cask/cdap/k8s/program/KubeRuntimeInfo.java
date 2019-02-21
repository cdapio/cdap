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

import co.cask.cdap.master.spi.program.ProgramController;
import co.cask.cdap.master.spi.program.RuntimeInfo;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import org.apache.twill.api.RunId;

import javax.annotation.Nullable;

/**
 * Kubernetes runtime info.
 */
public class KubeRuntimeInfo implements RuntimeInfo {
  private final ProgramController programController;

  public KubeRuntimeInfo(ProgramController programController) {
    this.programController = programController;
  }

  @Override
  public ProgramController getController() {
    return programController;
  }

  @Override
  public ProgramType getType() {
    return programController.getProgramRunId().getType();
  }

  @Override
  public ProgramId getProgramId() {
    return programController.getProgramRunId().getParent();
  }

  @Nullable
  @Override
  public RunId getTwillRunId() {
    return null;
  }
}
