/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
package io.cdap.cdap.proto.id;

import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.proto.BatchProgram;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.element.EntityType;
import org.apache.twill.api.RunId;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a program.
 */
public class ProgramId extends NamespacedEntityId implements ParentedId<ApplicationId> {
  private final String application;
  private final String version;
  private final ProgramType type;
  private final String program;
  private transient Integer hashCode;

  public ProgramId(String namespace, String application, ProgramType type, String program) {
    this(new ApplicationId(namespace, application), type, program);
  }

  public ProgramId(String namespace, String application, String version, ProgramType type, String program) {
    this(new ApplicationId(namespace, application, version), type, program);
  }

  public ProgramId(ApplicationId appId, ProgramType type, String program) {
    super(appId.getNamespace(), EntityType.PROGRAM);
    if (type == null) {
      throw new NullPointerException("Program type cannot be null.");
    }
    if (program == null) {
      throw new NullPointerException("Program ID cannot be null.");
    }
    this.application = appId.getApplication();
    this.version = appId.getVersion();
    this.type = type;
    this.program = program;
  }

  public ProgramId(String namespace, String application, String type, String program) {
    this(namespace, application, ProgramType.valueOf(type.toUpperCase()), program);
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
    return getProgram();
  }

  @Override
  public MetadataEntity toMetadataEntity() {
    return MetadataEntity.builder().append(MetadataEntity.NAMESPACE, namespace)
      .append(MetadataEntity.APPLICATION, application)
      .append(MetadataEntity.VERSION, version)
      .append(MetadataEntity.TYPE, type.getPrettyName())
      .appendAsType(MetadataEntity.PROGRAM, program)
      .build();
  }

  @Override
  public ApplicationId getParent() {
    return new ApplicationId(namespace, application, version);
  }

  public ApplicationReference getAppReference() {
    return new ApplicationReference(namespace, application);
  }

  public ProgramReference getProgramReference() {
    return new ProgramReference(getAppReference(), getType(), getProgram());
  }

  public BatchProgram getBatchProgram() {
    return new BatchProgram(application, type, program);
  }

  /**
   * Creates a {@link ProgramRunId} of this program id with the given run id.
   */
  public ProgramRunId run(String run) {
    return new ProgramRunId(new ApplicationId(getNamespace(), getApplication(), getVersion()), type, program, run);
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
    return Objects.equals(getParent(), programId.getParent()) &&
      Objects.equals(type, programId.type) &&
      Objects.equals(program, programId.program);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), getNamespace(), getApplication(), getVersion(),
                                              type, program);
    }
    return hashCode;
  }

  @SuppressWarnings("unused")
  public static ProgramId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new ProgramId(
      new ApplicationId(next(iterator, "namespace"), next(iterator, "application"), next(iterator, "version")),
      ProgramType.valueOfPrettyName(next(iterator, "type")),
      nextAndEnd(iterator, "program"));
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(
      Arrays.asList(getNamespace(), getApplication(), getVersion(), type.getPrettyName().toLowerCase(), program));
  }

  public static ProgramId fromString(String string) {
    return EntityId.fromString(string, ProgramId.class);
  }
}
