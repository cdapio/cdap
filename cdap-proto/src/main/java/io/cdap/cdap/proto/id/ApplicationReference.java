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
 * Identifies a versionless application.
 */
public class ApplicationReference extends NamespacedEntityId implements ParentedId<NamespaceId> {

  private final String application;
  private transient Integer hashCode;

  public ApplicationReference(String namespace, String application) {
    super(namespace, EntityType.APPLICATIONREFERENCE);
    if (application == null) {
      throw new NullPointerException("Application ID cannot be null.");
    }
    ensureValidId("application", application);
    this.application = application;
  }

  public ApplicationReference(NamespaceId namespace, String application) {
    this(namespace.getNamespace(), application);
  }

  public String getApplication() {
    return application;
  }

  public ApplicationId app(String version) {
    return new ApplicationId(namespace, application, version);
  }

  public ProgramReference program(ProgramType type, String program) {
    return new ProgramReference(this, type, program);
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, application));
  }

  @Override
  public String getEntityName() {
    return getApplication();
  }

  @Override
  public MetadataEntity toMetadataEntity() {
    return MetadataEntity.builder().append(MetadataEntity.NAMESPACE, namespace)
        .appendAsType(MetadataEntity.APPLICATION, application)
        .build();
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    ApplicationReference that = (ApplicationReference) o;
    return Objects.equals(namespace, that.namespace) && Objects.equals(application,
        that.application);
  }

  @SuppressWarnings("unused")
  public static ApplicationReference fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new ApplicationReference(next(iterator, "namespace"), next(iterator, "application"));
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, application);
    }
    return hashCode;
  }
}
