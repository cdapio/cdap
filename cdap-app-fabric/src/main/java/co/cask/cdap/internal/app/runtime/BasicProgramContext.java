/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.data.ProgramContext;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramRunId;

import javax.annotation.Nullable;

/**
 * Straight forward implementation of {@link ProgramContext}.
 */
public class BasicProgramContext implements ProgramContext {

  private final ProgramRunId programRunId;
  private final NamespacedEntityId componentId;

  public BasicProgramContext(ProgramRunId programRunId) {
    this(programRunId, null);
  }

  public BasicProgramContext(ProgramRunId programRunId, @Nullable NamespacedEntityId componentId) {
    this.programRunId = programRunId;
    this.componentId = componentId;
  }

  @Override
  public ProgramRunId getProgramRunId() {
    return programRunId;
  }

  @Nullable
  @Override
  public NamespacedEntityId getComponentId() {
    return componentId;
  }
}
