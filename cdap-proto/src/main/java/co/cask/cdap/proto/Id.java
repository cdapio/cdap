/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.EntityIdCompatible;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.FlowletQueueId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NotificationFeedId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.QueryId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.id.SystemServiceId;
import co.cask.cdap.proto.id.WorkflowId;

import java.nio.charset.Charset;
import java.util.regex.Pattern;

/**
 * Contains collection of classes representing different types of Ids.
 *
 * @deprecated As of 3.3.0, use {@link EntityId}.
 */
@Deprecated
public abstract class Id implements EntityIdCompatible {

  public static String getType(Class<? extends Id> type) {
    return type.getSimpleName().toLowerCase();
  }

  // Only allow alphanumeric and _ character for namespace
  private static final Pattern namespacePattern = Pattern.compile("[a-zA-Z0-9_]+");
  // Allow hyphens for other ids.
  private static final Pattern idPattern = Pattern.compile("[a-zA-Z0-9_-]+");
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

  private static boolean isValidDatasetId(String datasetId) {
    return datasetIdPattern.matcher(datasetId).matches();
  }

  public String getIdType() {
    return getType(this.getClass());
  }

  public static <T extends Id> T fromString(String string, Class<T> idClass) {
    return EntityId.fromStringOld(string, idClass);
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
   *
   * @deprecated As of 3.3.0, use {@link co.cask.cdap.proto.id.NamespacedId}.
   */
  @Deprecated
  public abstract static class NamespacedId extends Id {
    public abstract Namespace getNamespace();
  }

  /**
   * Uniquely identifies a Query Handle.
   *
   * @deprecated As of 3.3.0, use {@link QueryId}.
   */
  @Deprecated
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

    public String getId() {
      return id;
    }

    @Override
    public QueryId toEntityId() {
      return new QueryId(id);
    }
  }

  /**
   * Uniquely identifies a System Service.
   *
   * @deprecated As of 3.3.0, use {@link SystemServiceId}.
   */
  @Deprecated
  public static final class SystemService extends Id {
    private final String id;

    private SystemService(String id) {
      if (id == null) {
        throw new NullPointerException("id cannot be null.");
      }
      this.id = id;
    }

    public static SystemService from(String id) {
      return new SystemService(id);
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public SystemServiceId toEntityId() {
      return new SystemServiceId(id);
    }
  }

  /**
   * Uniquely identifies a Namespace.
   *
   * @deprecated As of 3.3.0, use {@link NamespaceId}.
   */
  @Deprecated
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
  }

  /**
   * Uniquely identifies an Application.
   *
   * @deprecated As of 3.3.0, use {@link ApplicationId}.
   */
  @Deprecated
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
  }


  /**
   * Uniquely identifies a Program run.
   *
   * @deprecated As of 3.3.0, use {@link ProgramRunId}.
   */
  @Deprecated
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
  }

  /**
   * Uniquely identifies a Program.
   *
   * @deprecated As of 3.3.0, use {@link ProgramId}.
   */
  @Deprecated
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
  }

  /**
   * Uniquely identifies a Worker.
   *
   * @deprecated As of 3.3.0, use {@link ProgramId}.
   */
  @Deprecated
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
  }

  /**
   * Uniquely identifies a Service.
   *
   * @deprecated As of 3.3.0, use {@link ProgramId}.
   */
  @Deprecated
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
  }

  /**
   * Uniquely identifies a Workflow.
   *
   * @deprecated As of 3.3.0, use {@link ProgramId}.
   */
  @Deprecated
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
  }

  /**
   * Uniquely identifies a Flow.
   *
   * @deprecated As of 3.3.0, use {@link ProgramId}.
   */
  @Deprecated
  public static class Flow extends Program {

    private Flow(Application application, String id) {
      super(application, ProgramType.FLOW, id);
    }

    public static Flow from(Application application, String flowId) {
      return new Flow(application, flowId);
    }

    public static Flow from(String appId, String flowId) {
      return new Flow(Id.Application.from(Namespace.DEFAULT, appId), flowId);
    }

    public static Flow from(String namespaceId, String appId, String flowId) {
      return new Flow(Id.Application.from(namespaceId, appId), flowId);
    }

    public static Flow from(Id.Namespace namespaceId, String appId, String flowId) {
      return new Flow(Id.Application.from(namespaceId, appId), flowId);
    }

    /**
     * Uniquely identifies a Flowlet.
     *
     * @deprecated As of 3.3.0, use {@link FlowletId}.
     */
    @Deprecated
    public static class Flowlet extends NamespacedId {

      private final Flow flow;
      private final String id;

      private Flowlet(Flow flow, String id) {
        if (flow == null) {
          throw new IllegalArgumentException("flow cannot be null");
        }
        if (id == null) {
          throw new IllegalArgumentException("id cannot be null");
        }
        this.flow = flow;
        this.id = id;
      }

      public static Flowlet from(Flow flow, String id) {
        return new Flowlet(flow, id);
      }

      public static Flowlet from(Application app, String flowId, String id) {
        return new Flowlet(new Flow(app, flowId), id);
      }

      @Override
      public Namespace getNamespace() {
        return flow.getNamespace();
      }

      @Override
      public String getId() {
        return id;
      }

      public Flow getFlow() {
        return flow;
      }

      @Override
      public FlowletId toEntityId() {
        return new FlowletId(flow.getNamespaceId(), flow.getApplicationId(), flow.getId(), id);
      }

      /**
       * Uniquely identifies a Flowlet Queue.
       *
       * @deprecated As of 3.3.0, use {@link FlowletQueueId}.
       */
      @Deprecated
      public static final class Queue extends NamespacedId {

        private final Flowlet producer;
        private final String id;

        public Queue(Flowlet producer, String id) {
          this.producer = producer;
          this.id = id;
        }

        public Flowlet getProducer() {
          return producer;
        }

        public String getId() {
          return id;
        }

        @Override
        public Namespace getNamespace() {
          return producer.getNamespace();
        }

        @Override
        public FlowletQueueId toEntityId() {
          return new FlowletQueueId(producer.getNamespace().getId(), producer.getFlow().getApplicationId(),
                                    producer.getFlow().getId(), producer.getId(), id);
        }
      }
    }
  }

  /**
   * Represents ID of a Schedule.
   *
   * @deprecated As of 3.3.0, use {@link ScheduleId}.
   */
  @Deprecated
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
  }

  /**
   * Represents ID of a Notification feed.
   *
   * @deprecated As of 3.3.0, use {@link NotificationFeedId}.
   */
  @Deprecated
  public static class NotificationFeed extends NamespacedId {

    private final Namespace namespace;
    private final String category;
    private final String name;

    private final String description;

    /**
     * {@link NotificationFeed} object from an id in the form of "namespace.category.name".
     *
     * @param id id of the notification feed to build
     * @return a {@link NotificationFeed} object which id is the same as {@code id}
     * @throws IllegalArgumentException when the id doesn't match a valid feed id
     */
    public static NotificationFeed fromId(String id) {
      String[] idParts = id.split("\\.");
      if (idParts.length != 3) {
        throw new IllegalArgumentException(String.format("Id %s is not a valid feed id.", id));
      }
      return new NotificationFeed(idParts[0], idParts[1], idParts[2], "");
    }

    public static NotificationFeed from(String namespace, String category, String name) {
      return new NotificationFeed(namespace, category, name, "");
    }

    private NotificationFeed(String namespace, String category, String name, String description) {
      if (namespace == null || namespace.isEmpty()) {
        throw new IllegalArgumentException("Namespace value cannot be null or empty.");
      }
      if (category == null || category.isEmpty()) {
        throw new IllegalArgumentException("Category value cannot be null or empty.");
      }
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Name value cannot be null or empty.");
      }
      if (!isValidId(namespace) || !isValidId(category) || !isValidId(name)) {
        throw new IllegalArgumentException("Namespace, category or name has a wrong format.");
      }

      this.namespace = Namespace.from(namespace);
      this.category = category;
      this.name = name;
      this.description = description;
    }

    public String getCategory() {
      return category;
    }

    @Override
    public String getId() {
      return name;
    }

    public String getFeedId() {
      return String.format("%s.%s.%s", namespace.getId(), category, name);
    }

    public String getNamespaceId() {
      return namespace.getId();
    }

    public String getName() {
      return name;
    }

    public String getDescription() {
      return description;
    }

    @Override
    public Namespace getNamespace() {
      return namespace;
    }

    @Override
    public NotificationFeedId toEntityId() {
      return new NotificationFeedId(namespace.getId(), category, name);
    }

    /**
     * Builder used to build {@link NotificationFeed}.
     */
    @Deprecated
    public static final class Builder {
      private String category;
      private String name;
      private String namespaceId;
      private String description;

      public Builder() {
        // No-op
      }

      public Builder(NotificationFeed feed) {
        this.namespaceId = feed.getNamespaceId();
        this.category = feed.getCategory();
        this.name = feed.getName();
        this.description = feed.getDescription();
      }

      public Builder setName(final String name) {
        this.name = name;
        return this;
      }

      public Builder setNamespaceId(final String namespace) {
        this.namespaceId = namespace;
        return this;
      }

      public Builder setDescription(final String description) {
        this.description = description;
        return this;
      }

      public Builder setCategory(final String category) {
        this.category = category;
        return this;
      }

      /**
       * @return a {@link NotificationFeed} object containing all the fields set in the builder.
       * @throws IllegalArgumentException if the namespaceId, category or name is invalid.
       */
      public NotificationFeed build() {
        return new NotificationFeed(namespaceId, category, name, description);
      }
    }
  }

  /**
   * Id.Stream uniquely identifies a stream.
   *
   * @deprecated As of 3.3.0, use {@link StreamId}.
   */
  @Deprecated
  public static final class Stream extends NamespacedId {
    private final Namespace namespace;
    private final String streamName;
    private transient int hashCode;

    private transient String id;
    private transient byte[] idBytes;

    private Stream(final Namespace namespace, final String streamName) {
      if (namespace == null) {
        throw new NullPointerException("Namespace cannot be null.");
      }
      if (streamName == null) {
        throw new IllegalArgumentException("Stream name cannot be null.");
      }
      if (!isValidId(streamName)) {
        throw new IllegalArgumentException(String.format("Stream name can only contain alphanumeric, " +
                                                           "'-' and '_' characters: %s", streamName));
      }

      this.namespace = namespace;
      this.streamName = streamName;
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
      return streamName;
    }

    public byte[] toBytes() {
      if (idBytes == null) {
        idBytes = toString().getBytes(Charset.forName("US-ASCII"));
      }
      return idBytes;
    }

    public static Stream from(Namespace id, String streamName) {
      return new Stream(id, streamName);
    }

    public static Stream from(String namespaceId, String streamName) {
      return from(Id.Namespace.from(namespaceId), streamName);
    }

    @Override
    public StreamId toEntityId() {
      return new StreamId(namespace.getId(), streamName);
    }

    /**
     * Uniquely identifies a stream view.
     *
     * @deprecated As of 3.3.0, use {@link StreamViewId}.
     */
    @Deprecated
    public static final class View extends NamespacedId {
      private final Stream stream;
      private final String id;

      public View(Stream stream, String id) {
        if (id == null) {
          throw new NullPointerException("ID cannot be null.");
        }
        if (!isValidId(id)) {
          throw new IllegalArgumentException(String.format("ID can only contain alphanumeric, " +
                                                             "'-' and '_' characters: %s", id));
        }
        this.stream = stream;
        this.id = id;
      }

      @Override
      public Namespace getNamespace() {
        return stream.getNamespace();
      }

      public String getNamespaceId() {
        return stream.getNamespace().getId();
      }

      public Id.Stream getStream() {
        return stream;
      }

      public String getStreamId() {
        return stream.getId();
      }

      @Override
      public String getId() {
        return id;
      }

      public static View from(Id.Stream streamId, String id) {
        return new View(streamId, id);
      }

      public static View from(Namespace namespace, String streamId, String id) {
        return new View(Id.Stream.from(namespace, streamId), id);
      }

      public static View from(String namespace, String streamId, String id) {
        return new View(Id.Stream.from(namespace, streamId), id);
      }

      @Override
      public StreamViewId toEntityId() {
        return new StreamViewId(stream.getNamespaceId(), stream.getId(), id);
      }
    }
  }

  /**
   * Dataset Type Id identifies a given dataset module.
   *
   * @deprecated As of 3.3.0, use {@link DatasetTypeId}.
   */
  @Deprecated
  public static final class DatasetType extends NamespacedId {
    private final Namespace namespace;
    private final String typeName;

    private DatasetType(Namespace namespace, String typeName) {
      if (namespace == null) {
        throw new NullPointerException("Namespace cannot be null.");
      }
      if (typeName == null) {
        throw new NullPointerException("Dataset type name cannot be null.");
      }
      if (!isValidDatasetId(typeName)) {
        throw new IllegalArgumentException("Invalid characters found in dataset type name '" + typeName +
                                             "'. Allowed characters are ASCII letters, numbers, and _, -, ., or $.");
      }
      this.namespace = namespace;
      this.typeName = typeName;
    }

    @Override
    public Namespace getNamespace() {
      return namespace;
    }

    public String getNamespaceId() {
      return namespace.getId();
    }

    public String getTypeName() {
      return typeName;
    }

    public static DatasetType from(Namespace id, String typeId) {
      return new DatasetType(id, typeId);
    }

    public static DatasetType from(String namespaceId, String typeId) {
      return new DatasetType(Namespace.from(namespaceId), typeId);
    }

    @Override
    public String getId() {
      return typeName;
    }

    @Override
    public DatasetTypeId toEntityId() {
      return new DatasetTypeId(namespace.getId(), typeName);
    }
  }

  /**
   * Dataset Module Id identifies a given dataset module.
   *
   * @deprecated As of 3.3.0, use {@link DatasetModuleId}.
   */
  @Deprecated
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
  }

  /**
   * Dataset Instance Id identifies a given dataset instance.
   *
   * @deprecated As of 3.3.0, use {@link DatasetId}.
   */
  @Deprecated
  public static final class DatasetInstance extends NamespacedId {
    private final Namespace namespace;
    private final String instanceId;

    private DatasetInstance(Namespace namespace, String instanceId) {
      if (namespace == null) {
        throw new NullPointerException("Namespace cannot be null.");
      }
      if (instanceId == null) {
        throw new NullPointerException("Dataset instance id cannot be null.");
      }
      if (!isValidDatasetId(instanceId)) {
        throw new IllegalArgumentException("Invalid characters found in dataset instance Id '" + instanceId +
                                             "'. Allowed characters are ASCII letters, numbers, and _, -, ., or $.");
      }
      this.namespace = namespace;
      this.instanceId = instanceId;
    }

    @Override
    public Namespace getNamespace() {
      return namespace;
    }

    public String getNamespaceId() {
      return namespace.getId();
    }

    public String getId() {
      return instanceId;
    }

    public static DatasetInstance from(Namespace id, String instanceId) {
      return new DatasetInstance(id, instanceId);
    }

    public static DatasetInstance from(String namespaceId, String instanceId) {
      return new DatasetInstance(Namespace.from(namespaceId), instanceId);
    }

    @Override
    public DatasetId toEntityId() {
      return new DatasetId(namespace.getId(), instanceId);
    }
  }

  /**
   * Artifact Id identifies an artifact by its namespace, name, and version.
   *
   * @deprecated As of 3.3.0, use {@link co.cask.cdap.api.artifact.ArtifactId}.
   */
  @Deprecated
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
      if (!isValidId(name)) {
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

    public co.cask.cdap.api.artifact.ArtifactId toArtifactId() {
      return new co.cask.cdap.api.artifact.ArtifactId(
        name, version, Namespace.SYSTEM.equals(namespace) ? ArtifactScope.SYSTEM : ArtifactScope.USER);
    }

    public static Artifact from(Namespace namespace, String name, String version) {
      return new Artifact(namespace, name, new ArtifactVersion(version));
    }

    public static Artifact from(Namespace namespace, String name, ArtifactVersion version) {
      return new Artifact(namespace, name, version);
    }

    public static Artifact from(Id.Namespace namespace, co.cask.cdap.api.artifact.ArtifactId id) {
      return new Artifact(ArtifactScope.SYSTEM.equals(id.getScope()) ? Namespace.SYSTEM : namespace,
                          id.getName(), id.getVersion());
    }

    /**
     * Parses a string expected to be of the form {name}-{version}.jar into an {@link co.cask.cdap.proto.Id.Artifact},
     * where name is a valid id and version is of the form expected by {@link ArtifactVersion}.
     *
     * @param namespace the namespace to use
     * @param fileName the string to parse
     * @return string parsed into an {@link co.cask.cdap.proto.Id.Artifact}
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
      return isValidId(name);
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
