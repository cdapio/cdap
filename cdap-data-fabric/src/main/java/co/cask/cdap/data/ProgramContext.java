/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.data;

import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramRunId;

import javax.annotation.Nullable;

/**
 * Represents the context for the program execution.
 */
public interface ProgramContext {

  /**
   * Returns the {@link ProgramRunId} of current running program.
   */
  ProgramRunId getProgramRunId();

  /**
   * Returns the component id of the current running program if it has one.
   */
  @Nullable
  NamespacedEntityId getComponentId();
}
