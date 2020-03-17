/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.runtimejob;

import java.util.Objects;

/**
 * Program run information.
 */
public class ProgramRunInfo {
  private final String namespace;
  private final String application;
  private final String program;
  private final String programType;
  private final String run;

  private ProgramRunInfo(String namespace, String application, String program, String programType, String run) {
    this.namespace = namespace;
    this.application = application;
    this.program = program;
    this.programType = programType;
    this.run = run;
  }

  /**
   * Builder to build program info.
   */
  public static class Builder {
    private String namespace;
    private String application;
    private String program;
    private String programType;
    private String run;

    /**
     * Sets namespace.
     *
     * @param namespace namespace
     * @return this builder
     */
    public Builder setNamespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * Sets application name.
     *
     * @param application application name
     * @return this builder
     */
    public Builder setApplication(String application) {
      this.application = application;
      return this;
    }

    /**
     * Sets program name.
     *
     * @param program program
     * @return this builder
     */
    public Builder setProgram(String program) {
      this.program = program;
      return this;
    }

    /**
     * Sets program type.
     *
     * @param programType program type
     * @return this builder
     */
    public Builder setProgramType(String programType) {
      this.programType = programType;
      return this;
    }

    /**
     * Sets program run.
     *
     * @param run program run
     * @return this builder
     */
    public Builder setRun(String run) {
      this.run = run;
      return this;
    }

    /**
     * Returns program run info.
     */
    public ProgramRunInfo build() {
      return new ProgramRunInfo(namespace, application, program, programType, run);
    }
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

  public String getProgramType() {
    return programType;
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
    ProgramRunInfo that = (ProgramRunInfo) o;
    return Objects.equals(namespace, that.namespace) && Objects.equals(application, that.application)
      && Objects.equals(program, that.program) && Objects.equals(run, that.run);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, application, program, run);
  }
}
