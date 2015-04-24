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

package co.cask.cdap.proto;

import co.cask.cdap.internal.Objects;

/**
 * Represents an adapter returned for /adapters/{adapter-id}.
 */
public class ProgramId {
  private final String namespace;
  private final String application;
  private final ProgramType type;
  private final String id;

  public ProgramId(Id.Program program) {
    this(program.getNamespaceId(), program.getApplicationId(), program.getType(), program.getId());
  }

  public ProgramId(String namespace, String application, ProgramType type, String id) {
    this.namespace = namespace;
    this.application = application;
    this.type = type;
    this.id = id;
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

  public String getId() {
    return id;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(namespace, application, type, id);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final ProgramId other = (ProgramId) obj;
    return Objects.equal(this.namespace, other.namespace) && Objects.equal(this.application, other.application) &&
      Objects.equal(this.type, other.type) && Objects.equal(this.id, other.id);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ProgramId{");
    sb.append("namespace='").append(namespace).append('\'');
    sb.append(", application='").append(application).append('\'');
    sb.append(", type=").append(type);
    sb.append(", id='").append(id).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
