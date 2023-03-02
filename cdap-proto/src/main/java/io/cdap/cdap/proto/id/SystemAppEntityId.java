/*
 * Copyright © 2015-2021 Cask Data, Inc.
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

import io.cdap.cdap.proto.element.EntityType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a system app entity.
 */
public class SystemAppEntityId extends NamespacedEntityId implements ParentedId<NamespaceId> {

  // defines the type of the system app entity, i.e, connection, draft, workspace...
  private String type;
  // the name of the entity
  private String name;
  // the system app name this entity is in, this is to distinguish similar entities
  // in different apps, for example, draft exists in pipeline service and
  // replication service, but they are different in these two services.
  private String appName;
  private transient Integer hashCode;

  public SystemAppEntityId(String namespace, String appName, String type, String name) {
    super(namespace, EntityType.SYSTEM_APP_ENTITY);
    if (type == null) {
      throw new NullPointerException("System App Entity Type cannot be null.");
    }
    if (name == null) {
      throw new NullPointerException("System App Entity Name cannot be null.");
    }
    if (appName == null) {
      throw new NullPointerException("System App Name cannot be null.");
    }
    this.type = type;
    this.name = name;
    this.appName = appName;
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, appName, type, name));
  }

  @SuppressWarnings("unused")
  public static SystemAppEntityId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new SystemAppEntityId(next(iterator, "namespace"), next(iterator, "appName"),
        next(iterator, "type"), nextAndEnd(iterator, "name"));
  }

  @Override
  public String getEntityName() {
    return getName();
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String getAppName() {
    return appName;
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, type, name, appName);
    }
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    SystemAppEntityId that = (SystemAppEntityId) o;
    return Objects.equals(namespace, that.namespace)
        && Objects.equals(appName, that.appName)
        && Objects.equals(type, that.type)
        && Objects.equals(name, that.name);
  }

  public static SystemAppEntityId fromString(String string) {
    return EntityId.fromString(string, SystemAppEntityId.class);
  }
}

