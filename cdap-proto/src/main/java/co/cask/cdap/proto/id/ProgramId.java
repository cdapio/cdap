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
import org.apache.twill.api.RunId;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a program.
 */
public class ProgramId extends EntityId implements NamespacedId, ParentedId<ApplicationId> {
  private final String namespace;
  private final String application;
  private final ProgramType type;
  private final String program;
  private transient Integer hashCode;

  public ProgramId(String namespace, String application, ProgramType type, String program) {
    super(EntityType.PROGRAM);
    this.namespace = namespace;
    this.application = application;
    this.type = type;
    this.program = program;
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

  public NamespaceId getNamespaceId() {
    return new NamespaceId(namespace);
  }

  @Override
  public ApplicationId getParent() {
    return new ApplicationId(namespace, application);
  }

  public FlowletId flowlet(String flowlet) {
    if (type != ProgramType.FLOW) {
      throw new IllegalArgumentException("Expected program type for flowlet to be " + ProgramType.FLOW);
    }
    return new FlowletId(namespace, application, program, flowlet);
  }

  /**
   * Creates a {@link ProgramRunId} of this program id with the given run id.
   */
  public ProgramRunId run(String run) {
    return new ProgramRunId(namespace, application, type, program, run);
  }

  /**
   * Creates a {@link ProgramRunId} of this program id with the given {@link RunId}.
   */
  public ProgramRunId run(RunId runId) {
    return run(runId.getId());
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    ProgramId programId = (ProgramId) o;
    return Objects.equals(namespace, programId.namespace) &&
      Objects.equals(application, programId.application) &&
      Objects.equals(type, programId.type) &&
      Objects.equals(program, programId.program);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, application, type, program);
    }
    return hashCode;
  }

  @Override
  public Id.Program toId() {
    return Id.Program.from(namespace, application, type, program);
  }

  @SuppressWarnings("unused")
  public static ProgramId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new ProgramId(
      next(iterator, "namespace"), next(iterator, "category"),
      ProgramType.valueOfPrettyName(next(iterator, "type")),
      nextAndEnd(iterator, "program"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return Collections.unmodifiableList(
      Arrays.asList(namespace, application, type.getPrettyName().toLowerCase(), program)
    );
  }

  public static ProgramId fromString(String string) {
    return EntityId.fromString(string, ProgramId.class);
  }
}
