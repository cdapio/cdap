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
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.app.AppVersion;
import io.cdap.cdap.proto.element.EntityType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies an application.
 */
public class ApplicationId extends NamespacedEntityId implements ParentedId<NamespaceId> {

  private final String application;
  private final String version;
  private transient Integer hashCode;
  public static final String DEFAULT_VERSION = "-SNAPSHOT";

  public ApplicationId(String namespace, String application) {
    this(namespace, application, DEFAULT_VERSION);
  }

  public ApplicationId(String namespace, String application, String version) {
    super(namespace, EntityType.APPLICATION);
    if (application == null) {
      throw new NullPointerException("Application ID cannot be null.");
    }
    if (version == null) {
      throw new NullPointerException("Version cannot be null.");
    }
    ensureValidId("application", application);
    this.application = application;
    this.version = version;
  }

  public String getApplication() {
    return application;
  }

  public String getVersion() {
    return version;
  }

  @Override
  public String getEntityName() {
    return getApplication();
  }

  @Override
  public MetadataEntity toMetadataEntity() {
    return MetadataEntity.builder().append(MetadataEntity.NAMESPACE, namespace)
        .appendAsType(MetadataEntity.APPLICATION, application)
        .append(MetadataEntity.VERSION, version)
        .build();
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
  }

  public ApplicationReference getAppReference() {
    return new ApplicationReference(namespace, application);
  }

  public AppVersion getAppVersion() {
    return new AppVersion(application, version);
  }

  public ProgramId program(ProgramType type, String program) {
    return new ProgramId(this, type, program);
  }

  public WorkflowId workflow(String program) {
    return new WorkflowId(this, program);
  }

  public ProgramId mr(String program) {
    return new ProgramId(this, ProgramType.MAPREDUCE, program);
  }

  public ProgramId spark(String program) {
    return new ProgramId(this, ProgramType.SPARK, program);
  }

  public ProgramId worker(String program) {
    return new ProgramId(this, ProgramType.WORKER, program);
  }

  public ServiceId service(String program) {
    return new ServiceId(this, program);
  }

  public ScheduleId schedule(String schedule) {
    return new ScheduleId(this, schedule);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    ApplicationId that = (ApplicationId) o;
    return Objects.equals(namespace, that.namespace)
        && Objects.equals(application, that.application)
        && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, application, version);
    }
    return hashCode;
  }

  @SuppressWarnings("unused")
  public static ApplicationId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new ApplicationId(next(iterator, "namespace"), next(iterator, "application"),
        nextAndEnd(iterator, "version"));
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, application, version));
  }

  public static ApplicationId fromString(String string) {
    return EntityId.fromString(string, ApplicationId.class);
  }
}
