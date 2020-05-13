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

package io.cdap.cdap.runtime.spi.provisioner;

import io.cdap.cdap.runtime.spi.ProgramRunInfo;

import java.util.Objects;

/**
 * A program run.
 *
 * @deprecated Since 6.2.0. Use {@link ProgramRunInfo} instead.
 */
@Deprecated
public class ProgramRun {
  private final String namespace;
  private final String application;
  private final String program;
  private final String run;

  public ProgramRun(String namespace, String application, String program, String run) {
    this.namespace = namespace;
    this.application = application;
    this.program = program;
    this.run = run;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getApplication() {
    return application;
  }

  public String getProgram() {
    return program;
  }

  public String getRun() {
    return run;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProgramRun that = (ProgramRun) o;

    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(application, that.application) &&
      Objects.equals(program, that.program) &&
      Objects.equals(run, that.run);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, application, program, run);
  }
}
