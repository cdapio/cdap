/*
 * Copyright © 2016 Cask Data, Inc.
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
package io.cdap.cdap.api;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents the runtime status of program.
 */
public enum ProgramStatus {
  INITIALIZING,
  RUNNING,
  STOPPING,
  COMPLETED,
  FAILED,
  KILLED;

  public static final Set<ProgramStatus> TERMINAL_STATES = new HashSet<>(Arrays.asList(COMPLETED, FAILED, KILLED));
}
