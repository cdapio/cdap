/*
 * Copyright © 2014 Cask Data, Inc.
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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Contains collection of classes representing different types of Ids.
 */
public final class Id  {

  /**
   * Represents ID of a namespace.
   */
  public static final class Namespace {
    private final String id;

    public Namespace(String id) {
      Preconditions.checkNotNull(id, "Namespace cannot be null.");
      this.id = id;
    }

    public String getId() {
      return id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      return id.equals(((Namespace) o).id);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id);
    }

    public static Namespace from(String namespace) {
      return new Namespace(namespace);
    }
  }

  /**
   * Program Id identifies a given application.
   * Application is global unique if used within context of namespace.
   */
  public static final class Application {
    private final Namespace namespace;
    private final String applicationId;

    public Application(final Namespace namespace, final String applicationId) {
      Preconditions.checkNotNull(namespace, "Namespace cannot be null.");
      Preconditions.checkNotNull(applicationId, "Application cannot be null.");
      this.namespace = namespace;
      this.applicationId = applicationId;
    }

    public Namespace getNamespace() {
      return namespace;
    }

    public String getNamespaceId() {
      return namespace.getId();
    }

    public String getId() {
      return applicationId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Application that = (Application) o;
      return namespace.equals(that.namespace) && applicationId.equals(that.applicationId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(namespace, applicationId);
    }

    public static Application from(Namespace id, String application) {
      return new Application(id, application);
    }

    public static Application from(String namespaceId, String applicationId) {
      return new Application(Namespace.from(namespaceId), applicationId);
    }
  }

  /**
   * Program Id identifies a given program.
   * Program is global unique if used within context of namespace and application.
   */
  public static class Program {
    private final Application application;
    private final String id;

    public Program(Application application, final String id) {
      Preconditions.checkNotNull(application, "Application cannot be null.");
      Preconditions.checkNotNull(id, "Id cannot be null.");
      this.application = application;
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public String getApplicationId() {
      return application.getId();
    }

    public String getNamespaceId() {
      return application.getNamespaceId();
    }

    public Application getApplication() {
      return application;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Program program = (Program) o;
      return application.equals(program.application) && id.equals(program.id);
    }

    @Override
    public int hashCode() {
      int result = application.hashCode();
      result = 31 * result + id.hashCode();
      return result;
    }

    public static Program from(Application appId, String pgmId) {
      return new Program(appId, pgmId);
    }

    public static Program from(String namespaceId, String appId, String pgmId) {
      return new Program(new Application(new Namespace(namespaceId), appId), pgmId);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("ProgramId(");

      sb.append("namespaceId:");
      if (this.application.getNamespaceId() == null) {
        sb.append("null");
      } else {
        sb.append(this.application.getNamespaceId());
      }
      sb.append(", applicationId:");
      if (this.application.getId() == null) {
        sb.append("null");
      } else {
        sb.append(this.application.getId());
      }
      sb.append(", runnableId:");
      if (this.id == null) {
        sb.append("null");
      } else {
        sb.append(this.id);
      }
      sb.append(")");
      return sb.toString();
    }
  }
}
