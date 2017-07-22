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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.proto.id.ProgramId;

/**
 *
 */
public final class SimpleProgramOptions implements ProgramOptions {

  private final ProgramId programId;
  private final Arguments arguments;
  private final Arguments userArguments;
  private final boolean debug;

  public SimpleProgramOptions(ProgramId programId) {
    this(programId, new BasicArguments(), new BasicArguments());
  }

  public SimpleProgramOptions(ProgramId programId, Arguments arguments, Arguments userArguments) {
    this(programId, arguments, userArguments, false);
  }

  public SimpleProgramOptions(ProgramId programId, Arguments arguments, Arguments userArguments, boolean debug) {
    this.programId = programId;
    this.arguments = arguments;
    this.userArguments = userArguments;
    this.debug = debug;
  }

  @Override
  public ProgramId getProgramId() {
    return programId;
  }

  @Override
  public Arguments getArguments() {
    return arguments;
  }

  @Override
  public Arguments getUserArguments() {
    return userArguments;
  }

  @Override
  public boolean isDebug() {
    return debug;
  }
}
