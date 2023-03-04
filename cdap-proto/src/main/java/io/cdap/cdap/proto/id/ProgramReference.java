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
import io.cdap.cdap.proto.BatchProgram;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.element.EntityType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Identifies a versionless program.
 */
public class ProgramReference extends NamespacedEntityId implements
    ParentedId<ApplicationReference> {

  private final String application;
  private final ProgramType type;
  private final String program;
  private transient Integer hashCode;

  public ProgramReference(ApplicationReference appRef, ProgramType type, String program) {
    super(appRef.getNamespace(), EntityType.PROGRAMREFERENCE);
    if (type == null) {
      throw new NullPointerException("Program type cannot be null.");
    }
    if (program == null) {
      throw new NullPointerException("Program ID cannot be null.");
    }
    this.application = appRef.getApplication();
    this.type = type;
    this.program = program;
  }

  public ProgramReference(String namespace, String application, ProgramType type, String program) {
    this(new ApplicationReference(namespace, application), type, program);
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

  public BatchProgram getBatchProgram() {
    return new BatchProgram(application, type, program);
  }

  public ProgramId id(String version) {
    return new ProgramId(new ApplicationId(namespace, application, version), type, program);
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(
        Arrays.asList(namespace, application, type.getPrettyName().toLowerCase(), program));
  }

  @SuppressWarnings("unused")
  public static ProgramReference fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new ProgramReference(
        next(iterator, "namespace"), next(iterator, "application"),
        ProgramType.valueOfPrettyName(next(iterator, "type")),
        nextAndEnd(iterator, "program"));
  }

  @Override
  public String getEntityName() {
    return getProgram();
  }

  @Override
  public MetadataEntity toMetadataEntity() {
    return MetadataEntity.builder().append(MetadataEntity.NAMESPACE, namespace)
        .append(MetadataEntity.APPLICATION, application)
        .append(MetadataEntity.TYPE, type.getPrettyName())
        .appendAsType(MetadataEntity.PROGRAM, program)
        .build();
  }

  @Override
  public ApplicationReference getParent() {
    return new ApplicationReference(namespace, application);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    ProgramReference programRef = (ProgramReference) o;
    return Objects.equals(getParent(), programRef.getParent())
        && Objects.equals(type, programRef.type)
        && Objects.equals(program, programRef.program);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), getNamespace(), getApplication(),
          type, program);
    }
    return hashCode;
  }
}
