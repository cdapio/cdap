/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
package co.cask.cdap.proto.id;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.element.EntityType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a program run.
 */
public class ProgramRunId extends NamespacedEntityId implements ParentedId<ProgramId> {
  private final String application;
  private final String version;
  private final ProgramType type;
  private final String program;
  private final String run;
  private transient Integer hashCode;

  public ProgramRunId(String namespace, String application, ProgramType type, String program, String run) {
    this(new ApplicationId(namespace, application), type, program, run);
  }

  public ProgramRunId(ApplicationId appId, ProgramType type, String program, String run) {
    super(appId.getNamespace(), EntityType.PROGRAM_RUN);
    if (type == null) {
      throw new NullPointerException("Program type cannot be null.");
    }
    if (program == null) {
      throw new NullPointerException("Program ID cannot be null.");
    }
    if (run == null) {
      throw new NullPointerException("Run cannot be null.");
    }
    this.application = appId.getApplication();
    this.version = appId.getVersion();
    this.type = type;
    this.program = program;
    this.run = run;
  }

  public String getApplication() {
    return application;
  }

  public String getVersion() {
    return version;
  }

  public ProgramType getType() {
    return type;
  }

  public String getProgram() {
    return program;
  }

  @Override
  public String getEntityName() {
    return run;
  }

  public String getRun() {
    return run;
  }

  @Override
  public ProgramId getParent() {
    return new ProgramId(new ApplicationId(getNamespace(), getApplication(), getVersion()), getType(), getProgram());
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    ProgramRunId that = (ProgramRunId) o;
    return Objects.equals(getParent(), that.getParent()) &&
      Objects.equals(run, that.run);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), getNamespace(), getApplication(), getVersion(),
                                              getType(), getProgram(), run);
    }
    return hashCode;
  }

  @Override
  public Id.Program.Run toId() {
    return new Id.Program.Run(Id.Program.from(getNamespace(), getApplication(), getType(), getProgram()), run);
  }

  @SuppressWarnings("unused")
  public static ProgramRunId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new ProgramRunId(
      new ApplicationId(next(iterator, "namespace"), next(iterator, "application"), next(iterator, "version")),
      ProgramType.valueOfPrettyName(next(iterator, "type")),
      next(iterator, "program"), nextAndEnd(iterator, "run"));
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(
      Arrays.asList(getNamespace(), getApplication(), getVersion(), getType().getPrettyName().toLowerCase(),
                    getProgram(), run)
    );
  }

  public static ProgramRunId fromString(String string) {
    return EntityId.fromString(string, ProgramRunId.class);
  }
}
