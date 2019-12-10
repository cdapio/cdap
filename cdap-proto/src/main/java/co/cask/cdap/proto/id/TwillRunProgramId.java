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

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.proto.element.EntityType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a program run.
 */
public class TwillRunProgramId extends NamespacedEntityId implements ParentedId<ApplicationId> {
  private final ProgramId programId;
  private final String twillRunId;
  private transient Integer hashCode;

  public TwillRunProgramId(ProgramId programId, String twillRunId) {
    super(programId.getNamespace(), EntityType.PROGRAM);
    if (programId == null) {
      throw new NullPointerException("Program ID cannot be null.");
    }
    if (twillRunId == null) {
      throw new NullPointerException("Twill Run Id cannot be null.");
    }
    this.programId = programId;
    this.twillRunId = twillRunId;
  }

  public ProgramId getProgramId() {
    return programId;
  }

  @Override
  public String getEntityName() {
    return programId.getEntityName();
  }

  public String getTwillRunId() {
    return twillRunId;
  }

  @Override
  public ApplicationId getParent() {
    return programId.getParent();
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    TwillRunProgramId that = (TwillRunProgramId) o;
    return Objects.equals(getParent(), that.getParent()) &&
      Objects.equals(twillRunId, that.twillRunId);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), programId.getNamespace(), 
              programId.getApplication(), programId.getVersion(), programId.getType(),
              programId.getProgram(), twillRunId);
    }
    return hashCode;
  }


  @Override
  public MetadataEntity toMetadataEntity() {
    return MetadataEntity.builder().append(MetadataEntity.NAMESPACE, programId.getNamespace())
      .append(MetadataEntity.APPLICATION, programId.getApplication())
      .append(MetadataEntity.VERSION, programId.getVersion())
      .append(MetadataEntity.TYPE, programId.getType().getPrettyName())
      .append(MetadataEntity.PROGRAM, programId.getProgram())
      .appendAsType(MetadataEntity.PROGRAM_RUN, twillRunId)
      .build();
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(
      Arrays.asList(getNamespace(), programId.getApplication(), programId.getVersion(),
              programId.getType().getPrettyName().toLowerCase(),
              programId.getProgram(), twillRunId)
    );
  }
}
