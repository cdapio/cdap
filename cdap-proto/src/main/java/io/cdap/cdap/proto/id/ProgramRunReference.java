/*
 * Copyright © 2022 Cask Data, Inc.
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
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.element.EntityType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a program run without version
 */
public class ProgramRunReference extends NamespacedEntityId implements ParentedId<ProgramReference> {
  private final String application;
  private final ProgramType type;
  private final String program;
  private final String run;
  private transient Integer hashCode;

  public ProgramRunReference(ApplicationReference appRef, ProgramType type, String program, String run) {
    super(appRef.getNamespace(), EntityType.PROGRAMRUNREFERENCE);
    if (type == null) {
      throw new NullPointerException("Program type cannot be null.");
    }
    if (program == null) {
      throw new NullPointerException("Program ID cannot be null.");
    }
    if (run == null) {
      throw new NullPointerException("Run ID cannot be null.");
    }
    this.application = appRef.getApplication();
    this.type = type;
    this.program = program;
    this.run = run;
  }

  public ProgramRunReference(String namespace, String application, ProgramType type, String program, String run) {
    this(new ApplicationReference(namespace, application), type, program, run);
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

  public ProgramRunId version(String version) {
    return new ProgramRunId(new ApplicationId(namespace, application, version), type, program, run);
  }

  @Override
  public String getEntityName() {
    return run;
  }

  @Override
  public ProgramReference getParent() {
    return new ProgramReference(getNamespace(), getApplication(), getType(), getProgram());
  }

  public static ProgramRunReference fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new ProgramRunReference(
      new ApplicationReference(next(iterator, "namespace"), next(iterator, "application")),
      ProgramType.valueOfPrettyName(next(iterator, "type")),
      next(iterator, "program"), nextAndEnd(iterator, "run"));
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(
      Arrays.asList(namespace, application, type.getPrettyName().toLowerCase(), program, run));
  }

  @Override
  public MetadataEntity toMetadataEntity() {
    return MetadataEntity.builder()
      .append(MetadataEntity.NAMESPACE, namespace)
      .append(MetadataEntity.APPLICATION, application)
      .append(MetadataEntity.TYPE, type.getPrettyName())
      .append(MetadataEntity.PROGRAM, program)
      .appendAsType(MetadataEntity.PROGRAM_RUN, run)
      .build();
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    ProgramRunReference programRunRef = (ProgramRunReference) o;
    return Objects.equals(getParent(), programRunRef.getParent()) && Objects.equals(run, programRunRef.run);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), getNamespace(), getApplication(), type, program, run);
    }
    return hashCode;
  }
}
