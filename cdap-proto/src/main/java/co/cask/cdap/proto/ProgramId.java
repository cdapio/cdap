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

/**
 * Flat representation of {@link Id.Program}, for HTTP endpoints.
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
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProgramId programId = (ProgramId) o;

    if (application != null ? !application.equals(programId.application) :
         programId.application != null) {
      return false;
    }
    if (id != null ? !id.equals(programId.id) : programId.id != null) {
      return false;
    }
    if (namespace != null
         ? !namespace.equals(programId.namespace) : programId.namespace != null) {
      return false;
    }
    if (type != programId.type) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = namespace != null ? namespace.hashCode() : 0;
    result = 31 * result + (application != null ? application.hashCode() : 0);
    result = 31 * result + (type != null ? type.hashCode() : 0);
    result = 31 * result + (id != null ? id.hashCode() : 0);
    return result;
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
