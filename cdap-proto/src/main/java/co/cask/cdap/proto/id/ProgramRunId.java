/*
 * Copyright © 2015 Cask Data, Inc.
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
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a program run.
 */
public class ProgramRunId extends EntityId implements NamespacedId, ParentedId<ProgramId> {
  private final String namespace;
  private final String application;
  private final ProgramType type;
  private final String program;
  private final String run;

  public ProgramRunId(String namespace, String application, ProgramType type, String program, String run) {
    super(EntityType.PROGRAM_RUN);
    this.namespace = namespace;
    this.application = application;
    this.type = type;
    this.program = program;
    this.run = run;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getApplication() {
    return application;
  }

  public ProgramType getType() {
    return type;
  }

  public String getProgram() {
    return program;
  }

  public String getRun() {
    return run;
  }

  @Override
  public ProgramId getParent() {
    return new ProgramId(namespace, application, type, program);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    ProgramRunId that = (ProgramRunId) o;
    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(application, that.application) &&
      Objects.equals(type, that.type) &&
      Objects.equals(program, that.program) &&
      Objects.equals(run, that.run);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), namespace, application, type, program, run);
  }

  @Override
  public Id toId() {
    return new Id.Program.Run(Id.Program.from(namespace, application, type, program), run);
  }

  @SuppressWarnings("unused")
  public static ProgramRunId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new ProgramRunId(
      next(iterator, "namespace"), next(iterator, "category"),
      ProgramType.valueOfPrettyName(next(iterator, "type")),
      next(iterator, "program"), nextAndEnd(iterator, "run"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return ImmutableList.of(namespace, application, type.getPrettyName().toLowerCase(), program, run);
  }

  public static ProgramRunId fromString(String string) {
    return EntityId.fromString(string, ProgramRunId.class);
  }
}
