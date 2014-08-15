/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.app.program.Program;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 *
 */
public final class ApplicationWithPrograms {
  private final ApplicationSpecLocation appSpecLoc;
  private final List<Program> programs;

  public ApplicationWithPrograms(ApplicationSpecLocation appSpecLoc, List<? extends Program> programs) {
    this.appSpecLoc = appSpecLoc;
    this.programs = ImmutableList.copyOf(programs);
  }

  public ApplicationSpecLocation getAppSpecLoc() {
    return appSpecLoc;
  }

  public Iterable<Program> getPrograms() {
    return programs;
  }
}
