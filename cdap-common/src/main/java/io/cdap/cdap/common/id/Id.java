/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.common.id;

import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.QueryId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.cdap.proto.id.WorkerId;
import io.cdap.cdap.proto.id.WorkflowId;

import java.util.regex.Pattern;

/**
 * Contains collection of classes representing different types of old Ids.
 */
public abstract class Id implements EntityIdCompatible {

  public static String getType(Class<? extends Id> type) {
    return type.getSimpleName().toLowerCase();
  }

  // Only allow alphanumeric and _ character for namespace
  private static final Pattern namespacePattern = Pattern.compile("[a-zA-Z0-9_]+");
  // Allow hyphens for other ids.
  private static final Pattern idPattern = Pattern.compile("[a-zA-Z0-9_-]+");
  // Allow '.' and hyphens for artifact ids.
  private static final Pattern artifactIdPattern = Pattern.compile("[\\.a-zA-Z0-9_-]+");
  // Allow '.' and '$' for dataset ids since they can be fully qualified class names
  private static final Pattern datasetIdPattern = Pattern.compile("[$\\.a-zA-Z0-9_-]+");

  private transient String toString;
  private transient Integer hashCode;

  private static boolean isValidNamespaceId(String name) {
    return namespacePattern.matcher(name).matches();
  }

  private static boolean isValidId(String name) {
    return idPattern.matcher(name).matches();
  }

  private static boolean isValidArtifactId(String name) {
    return artifactIdPattern.matcher(name).matches();
  }

  private static boolean isValidDatasetId(String datasetId) {
    return datasetIdPattern.matcher(datasetId).matches();
  }

  public String getIdType() {
    return getType(this.getClass());
  }

  @Override
  public final String toString() {
    if (toString == null) {
      toString = toEntityId().toString();
    }
    return toString;
  }

  @Override
  public final boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Id)) {
      return false;
    }

    Id other = (Id) obj;
    return this.toEntityId().equals(other.toEntityId());
  }

  @Override
  public final int hashCode() {
    if (hashCode == null) {
      hashCode = toEntityId().hashCode();
    }
    return hashCode;
  }

  public abstract String getId();

  /**
   * Indicates that the ID belongs to a namespace.
   */
  public abstract static class NamespacedId extends Id {
    public abstract Namespace getNamespace();
  }

  /**
   * Uniquely identifies a Query Handle.
   */
  public static final class QueryHandle extends Id {
    private final String id;

    private QueryHandle(String id) {
      if (id == null) {
        throw new NullPointerException("id cannot be null.");
      }
      this.id = id;
    }

    public static QueryHandle from(String id) {
      return new QueryHandle(id);
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public QueryId toEntityId() {
      return new QueryId(id);
    }
  }

  /**
   * Uniquely identifies a Namespace.
   */
  public static final class Namespace extends Id {
    public static final Namespace DEFAULT = from("default");
    public static final Namespace SYSTEM = from("system");
    public static final Namespace CDAP = from("cdap");

    private final String id;

    public Namespace(String id) {
      if (id == null) {
        throw new NullPointerException("Namespace cannot be null.");
      }
      if (!isValidNamespaceId(id)) {
        throw new IllegalArgumentException("Namespace '" + id + "' has an incorrect format.");
      }
      this.id = id;
    }

    @Override
    public String getId() {
      return id;
    }

    public static Namespace from(String namespace) {
      return new Namespace(namespace);
    }

    @Override
    public NamespaceId toEntityId() {
      return new NamespaceId(id);
    }

    public static Namespace fromEntityId(NamespaceId namespaceId) {
      return new Namespace(namespaceId.getNamespace());
    }
  }

  /**
   * Uniquely identifies an Application.
   */
  public static final class Application extends NamespacedId {
    private final Namespace namespace;
    private final String applicationId;

    public Application(final Namespace namespace, final String applicationId) {
      if (namespace == null) {
        throw new NullPointerException("Namespace cannot be null.");
      }
      if (applicationId == null) {
        throw new NullPointerException("Application cannot be null.");
      }
      if (!isValidId(applicationId)) {
        throw new IllegalArgumentException("Invalid Application ID.");
      }
      this.namespace = namespace;
      this.applicationId = applicationId;
    }

    @Override
    public Namespace getNamespace() {
      return namespace;
    }

    public String getNamespaceId() {
      return namespace.getId();
    }

    @Override
    public String getId() {
      return applicationId;
    }

    public static Application from(Namespace id, String applicationId) {
      return new Application(id, applicationId);
    }

    public static Application from(String namespaceId, String applicationId) {
      return new Application(Namespace.from(namespaceId), applicationId);
    }

    @Override
    public ApplicationId toEntityId() {
      return new ApplicationId(namespace.getId(), applicationId);
    }

    public static Application fromEntityId(ApplicationId applicationId) {
      return from(applicationId.getNamespace(), applicationId.getApplication());
    }
  }


  /**
   * Uniquely identifies a Program run.
   */
  public static class Run extends NamespacedId {

    private final Program program;
    private final String id;

    public Run(Program program, String id) {
      this.program = program;
      this.id = id;
    }

    public Program getProgram() {
      return program;
    }

    @Override
    public Namespace getNamespace() {
      return program.getNamespace();
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public ProgramRunId toEntityId() {
      return new ProgramRunId(program.getNamespaceId(), program.getApplicationId(), program.getType(),
                              program.getId(), id);
    }

    public static Run fromEntityId(ProgramRunId programRunId) {
      return new Run(Id.Program.fromEntityId(programRunId.getParent()), programRunId.getRun());
    }
  }

  /**
   * Uniquely identifies a Program.
   */
  public static class Program extends NamespacedId {
    private final Application application;
    private final ProgramType type;
    private final String id;

    public Program(Application application, ProgramType type, final String id) {
      if (application == null) {
        throw new NullPointerException("Application cannot be null.");
      }
      if (type == null) {
        throw new NullPointerException("Program type cannot be null.");
      }
      if (id == null) {
        throw new NullPointerException("Program id cannot be null.");
      }
      this.application = application;
      this.type = type;
      this.id = id;
    }

    @Override
    public String getId() {
      return id;
    }

    public ProgramType getType() {
      return type;
    }

    public String getApplicationId() {
      return application.getId();
    }

    public String getNamespaceId() {
      return application.getNamespaceId();
    }

    @Override
    public Namespace getNamespace() {
      return application.getNamespace();
    }

    public Application getApplication() {
      return application;
    }

    public static Program from(Application appId, ProgramType type, String pgmId) {
      return new Program(appId, type, pgmId);
    }

    public static Program from(Id.Namespace namespaceId, String appId, ProgramType type, String pgmId) {
      return new Program(new Application(namespaceId, appId), type, pgmId);
    }

    public static Program from(String namespaceId, String appId, ProgramType type, String pgmId) {
      return new Program(new Application(new Namespace(namespaceId), appId), type, pgmId);
    }

    @Override
    public ProgramId toEntityId() {
      return new ProgramId(application.getNamespaceId(), application.getId(), type, id);
    }

    public static Program fromEntityId(ProgramId programId) {
      return new Program(Id.Application.fromEntityId(programId.getParent()),
                         programId.getType(), programId.getProgram());
    }
  }

  /**
   * Uniquely identifies a Worker.
   */
  public static class Worker extends Program {

    private Worker(Application application, String id) {
      super(application, ProgramType.WORKER, id);
    }

    public static Worker from(Application application, String id) {
      return new Worker(application, id);
    }

    public static Worker from(Namespace namespace, String appId, String id) {
      return new Worker(new Application(namespace, appId), id);
    }

    public static Worker fromEntityId(WorkerId workerId) {
      return new Worker(Id.Application.fromEntityId(workerId.getParent()), workerId.getProgram());
    }
  }

  /**
   * Uniquely identifies a Service.
   */
  public static class Service extends Program {

    private Service(Application application, String id) {
      super(application, ProgramType.SERVICE, id);
    }

    public static Service from(Application application, String id) {
      return new Service(application, id);
    }

    public static Service from(Namespace namespace, String application, String id) {
      return new Service(Id.Application.from(namespace, application), id);
    }

    @Override
    public ServiceId toEntityId() {
      return new ServiceId(super.getNamespaceId(), super.getApplicationId(), super.getId());
    }

    public static Service fromEntityId(ServiceId serviceId) {
      return new Service(Id.Application.fromEntityId(serviceId.getParent()), serviceId.getProgram());
    }
  }

  /**
   * Uniquely identifies a Workflow.
   */
  public static class Workflow extends Program {

    private Workflow(Application application, String id) {
      super(application, ProgramType.WORKFLOW, id);
    }

    public static Workflow from(Application application, String id) {
      return new Workflow(application, id);
    }

    public static Workflow from(Namespace namespace, String application, String id) {
      return new Workflow(Id.Application.from(namespace, application), id);
    }

    @Override
    public WorkflowId toEntityId() {
      return new WorkflowId(super.getNamespaceId(), super.getApplicationId(), super.getId());
    }

    public static Workflow fromEntityId(WorkflowId workflowId) {
      return new Workflow(Id.Application.fromEntityId(workflowId.getParent()), workflowId.getProgram());
    }
  }

  /**
   * Represents ID of a Schedule.
   */
  public static class Schedule extends NamespacedId {

    private final Application application;
    private final String id;

    private Schedule(Application application, String id) {
      if (application == null) {
        throw new IllegalArgumentException("application cannot be null");
      }
      if (id == null || id.isEmpty()) {
        throw new IllegalArgumentException("id cannot be null or empty");
      }
      this.application = application;
      this.id = id;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public Namespace getNamespace() {
      return application.getNamespace();
    }

    public Application getApplication() {
      return application;
    }

    public static Schedule from(Application application, String id) {
      return new Schedule(application, id);
    }

    public static Schedule from(Namespace namespace, String appId, String id) {
      return new Schedule(Id.Application.from(namespace, appId), id);
    }

    @Override
    public ScheduleId toEntityId() {
      return new ScheduleId(application.getNamespaceId(), application.getId(), id);
    }

    public static Schedule fromEntityId(ScheduleId scheduleId) {
      return from(Id.Application.fromEntityId(scheduleId.getParent()), scheduleId.getSchedule());
    }
  }

  /**
   * Dataset Module Id identifies a given dataset module.
   */
  public static final class DatasetModule extends NamespacedId {
    private final Namespace namespace;
    private final String moduleId;

    private DatasetModule(Namespace namespace, String moduleId) {
      if (namespace == null) {
        throw new NullPointerException("Namespace cannot be null.");
      }
      if (moduleId == null) {
        throw new NullPointerException("Dataset module id cannot be null.");
      }
      if (!isValidDatasetId(moduleId)) {
        throw new IllegalArgumentException("Invalid characters found in dataset module Id '" + moduleId +
                                             "'. Allowed characters are ASCII letters, numbers, and _, -, ., or $.");
      }
      this.namespace = namespace;
      this.moduleId = moduleId;
    }

    @Override
    public Namespace getNamespace() {
      return namespace;
    }

    public String getNamespaceId() {
      return namespace.getId();
    }

    @Override
    public String getId() {
      return moduleId;
    }

    public static DatasetModule from(Namespace id, String moduleId) {
      return new DatasetModule(id, moduleId);
    }

    public static DatasetModule from(String namespaceId, String moduleId) {
      return new DatasetModule(Namespace.from(namespaceId), moduleId);
    }

    @Override
    public DatasetModuleId toEntityId() {
      return new DatasetModuleId(namespace.getId(), moduleId);
    }

    public static DatasetModule fromEntityId(DatasetModuleId moduleId) {
      return from(Id.Namespace.fromEntityId(moduleId.getNamespaceId()), moduleId.getModule());
    }
  }

  /**
   * Artifact Id identifies an artifact by its namespace, name, and version.
   */
  public static class Artifact extends NamespacedId implements Comparable<Artifact> {
    private final Namespace namespace;
    private final String name;
    private final ArtifactVersion version;

    public Artifact(Namespace namespace, String name, ArtifactVersion version) {
      if (namespace == null) {
        throw new NullPointerException("Namespace cannot be null.");
      }
      if (name == null) {
        throw new NullPointerException("Name cannot be null.");
      }
      if (!isValidArtifactId(name)) {
        throw new IllegalArgumentException("Invalid artifact name.");
      }
      if (version == null) {
        throw new NullPointerException("Version cannot be null.");
      }
      if (version.getVersion() == null) {
        throw new NullPointerException("Invalid artifact version.");
      }
      this.namespace = namespace;
      this.name = name;
      this.version = version;
    }
    public Artifact(String namespace, String name, ArtifactVersion version) {
      this(new Namespace(namespace), name, version);
    }

    public Artifact(String namespace, String name, String version) {
      this(new Namespace(namespace), name, new ArtifactVersion(version));
    }

    @Override
    public Namespace getNamespace() {
      return namespace;
    }

    public String getName() {
      return name;
    }

    public ArtifactVersion getVersion() {
      return version;
    }

    @Override
    public String getId() {
      return String.format("%s-%s", name, version.getVersion());
    }

    public io.cdap.cdap.api.artifact.ArtifactId toArtifactId() {
      return new io.cdap.cdap.api.artifact.ArtifactId(
        name, version, Namespace.SYSTEM.equals(namespace) ? ArtifactScope.SYSTEM : ArtifactScope.USER);
    }

    public static Artifact from(Namespace namespace, String name, String version) {
      return new Artifact(namespace, name, new ArtifactVersion(version));
    }

    public static Artifact from(Namespace namespace, String name, ArtifactVersion version) {
      return new Artifact(namespace, name, version);
    }

    public static Artifact from(Id.Namespace namespace, io.cdap.cdap.api.artifact.ArtifactId id) {
      return new Artifact(ArtifactScope.SYSTEM.equals(id.getScope()) ? Namespace.SYSTEM : namespace,
                          id.getName(), id.getVersion());
    }

    public static Artifact fromEntityId(ArtifactId artifactId) {
      return from(Namespace.fromEntityId(artifactId.getParent()), artifactId.getArtifact(), artifactId.getVersion());
    }

    /**
     * Parses a string expected to be of the form {name}-{version}.jar into an {@link Id.Artifact},
     * where name is a valid id and version is of the form expected by {@link ArtifactVersion}.
     *
     * @param namespace the namespace to use
     * @param fileName the string to parse
     * @return string parsed into an {@link Id.Artifact}
     * @throws IllegalArgumentException if the string is not in the expected format
     */
    public static Artifact parse(Id.Namespace namespace, String fileName) {
      if (!fileName.endsWith(".jar")) {
        throw new IllegalArgumentException(String.format("Artifact name '%s' does not end in .jar", fileName));
      }

      // strip '.jar' from the filename
      fileName = fileName.substring(0, fileName.length() - ".jar".length());

      // true means try and match version as the end of the string
      ArtifactVersion artifactVersion = new ArtifactVersion(fileName, true);
      String rawVersion = artifactVersion.getVersion();
      // this happens if it could not parse the version
      if (rawVersion == null) {
        throw new IllegalArgumentException(
          String.format("Artifact name '%s' is not of the form {name}-{version}.jar", fileName));
      }

      // filename should be {name}-{version}.  Strip -{version} from it to get artifact name
      String artifactName = fileName.substring(0, fileName.length() - rawVersion.length() - 1);
      return Id.Artifact.from(namespace, artifactName, rawVersion);
    }

    public static boolean isValidName(String name) {
      return isValidArtifactId(name);
    }

    @Override
    public int compareTo(Artifact o) {
      int code = namespace.getId().compareTo(o.namespace.getId());
      if (code != 0) {
        return code;
      }
      code = name.compareTo(o.name);
      if (code != 0) {
        return code;
      }
      code = version.compareTo(o.version);
      return code;
    }

    @Override
    public ArtifactId toEntityId() {
      return new ArtifactId(namespace.getId(), name, version.getVersion());
    }
  }
}
