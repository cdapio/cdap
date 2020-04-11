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

package io.cdap.cdap.runtime.spi;

import java.util.Objects;

/**
 * Program run information.
 */
public class ProgramRunInfo {
  private final String namespace;
  private final String application;
  private final String version;
  private final String programType;
  private final String program;
  private final String run;

  private ProgramRunInfo(String namespace, String application,
                         String version, String programType, String program, String run) {
    this.namespace = namespace;
    this.application = application;
    this.version = version;
    this.programType = programType;
    this.program = program;
    this.run = run;
  }

  /**
   * Builder to build program info.
   */
  public static class Builder {
    private String namespace;
    private String application;
    private String version;
    private String programType;
    private String program;
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
     * Sets the application version.
     *
     * @param version the application version
     * @return this builder
     */
    public Builder setVersion(String version) {
      this.version = version;
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
      if (namespace == null) {
        throw new IllegalArgumentException("Missing namespace");
      }
      if (application == null) {
        throw new IllegalArgumentException("Missing application");
      }
      if (version == null) {
        throw new IllegalArgumentException("Missing version");
      }
      if (programType == null) {
        throw new IllegalArgumentException("Missing program type");
      }
      if (program == null) {
        throw new IllegalArgumentException("Missing program");
      }
      if (run == null) {
        throw new IllegalArgumentException("Missing run");
      }
      return new ProgramRunInfo(namespace, application, version, programType, program, run);
    }
  }

  public String getNamespace() {
    return namespace;
  }

  public String getApplication() {
    return application;
  }

  public String getVersion() {
    return version;
  }

  public String getProgramType() {
    return programType;
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
    ProgramRunInfo that = (ProgramRunInfo) o;
    return namespace.equals(that.namespace) &&
      application.equals(that.application) &&
      version.equals(that.version) &&
      programType.equals(that.programType) &&
      program.equals(that.program) &&
      run.equals(that.run);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, application, version, programType, program, run);
  }

  @Override
  public String toString() {
    return "ProgramRunInfo{" +
      "namespace='" + namespace + '\'' +
      ", application='" + application + '\'' +
      ", version='" + version + '\'' +
      ", programType='" + programType + '\'' +
      ", program='" + program + '\'' +
      ", run='" + run + '\'' +
      '}';
  }
}
