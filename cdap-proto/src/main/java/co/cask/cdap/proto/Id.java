/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

/**
 * Contains collection of classes representing different types of Ids.
 */
public abstract class Id {

  private static final CharMatcher namespaceMatcher =
    CharMatcher.inRange('A', 'Z')
    .or(CharMatcher.inRange('a', 'z'))
    .or(CharMatcher.inRange('0', '9'))
    .or(CharMatcher.is('_'));
  // Allow hyphens for other ids.
  private static final CharMatcher idMatcher = namespaceMatcher.or(CharMatcher.is('-'));
  // Allow '.' and '$' for dataset ids since they can be fully qualified class names
  private static final CharMatcher datasetIdCharMatcher = idMatcher.or(CharMatcher.is('.')).or(CharMatcher.is('$'));

  private static boolean isValidNamespaceId(String name) {
    return namespaceMatcher.matchesAllOf(name);
  }

  private static boolean isValidId(String name) {
    return idMatcher.matchesAllOf(name);
  }

  private static boolean isValidDatasetId(String datasetId) {
    return datasetIdCharMatcher.matchesAllOf(datasetId);
  }

  public static String getIdType(Class<? extends Id> cls) {
    return cls.getSimpleName().toLowerCase();
  }

  public static boolean isType(String string, Class<? extends Id> expectedType) {
    String[] parts = string.split(":", 2);
    return parts.length == 2 && getIdType(expectedType).equals(parts[0]);
  }

  public String getIdType() {
    return getIdType(getClass());
  }

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
      Preconditions.checkNotNull(id, "id cannot be null.");
      this.id = id;
    }

    public static QueryHandle from(String id) {
      return new QueryHandle(id);
    }

    @Override
    public String toString() {
      return String.format("%s:%s", getIdType(), id);
    }

    public String getId() {
      return id;
    }
  }

  /**
   * Uniquely identifies a System Service.
   */
  public static final class SystemService extends Id {
    private final String id;

    private SystemService(String id) {
      Preconditions.checkNotNull(id, "id cannot be null.");
      this.id = id;
    }

    public static SystemService from(String id) {
      return new SystemService(id);
    }

    @Override
    public String toString() {
      return String.format("%s:%s", getIdType(), id);
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
      Preconditions.checkNotNull(id, "Namespace '" + id + "' cannot be null.");
      Preconditions.checkArgument(isValidNamespaceId(id), "Namespace '" + id + "' has an incorrect format.");
      this.id = id;
    }

    public static Namespace from(String namespace) {
      return new Namespace(namespace);
    }

    public String getId() {
      return id;
    }

    @Override
    public String toString() {
      return String.format("%s:%s", getIdType(), id);
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
      return id.hashCode();
    }
  }

  /**
   * Uniquely identifies an Application.
   */
  public static final class Application extends NamespacedId {
    private final Namespace namespace;
    private final String applicationId;

    public Application(final Namespace namespace, final String applicationId) {
      Preconditions.checkNotNull(namespace, "Namespace cannot be null.");
      Preconditions.checkNotNull(applicationId, "Application cannot be null.");
      Preconditions.checkArgument(isValidId(applicationId), "Invalid Application ID.");
      this.namespace = namespace;
      this.applicationId = applicationId;
    }

    public static Application from(Namespace id, String applicationId) {
      return new Application(id, applicationId);
    }

    public static Application from(String namespaceId, String applicationId) {
      return new Application(Namespace.from(namespaceId), applicationId);
    }

    @Override
    public Namespace getNamespace() {
      return namespace;
    }

    @Override
    public String toString() {
      return String.format("%s:%s.%s", getIdType(), namespace.getId(), applicationId);
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

    @Override
    public String toString() {
      return String.format("%s:%s.%s.%s.%s.%s", getIdType(),
                           program.getNamespaceId(),
                           program.getApplicationId(),
                           program.getType().getPrettyName().toLowerCase(), program.getId(), id);
    }

    public Program getProgram() {
      return program;
    }

    @Override
    public Namespace getNamespace() {
      return program.getNamespace();
    }

    public String getId() {
      return id;
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
      Preconditions.checkNotNull(application, "application cannot be null.");
      Preconditions.checkNotNull(application, "type cannot be null.");
      Preconditions.checkNotNull(id, "id cannot be null.");
      this.application = application;
      this.type = type;
      this.id = id;
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
    public String toString() {
      return String.format("%s:%s.%s.%s.%s", getIdType(),
                           application.getNamespaceId(),
                           application.getId(),
                           type.getPrettyName().toLowerCase(),
                           id);
    }

    @Override
    public String getIdType() {
      return getIdType(Program.class);
    }

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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || !getClass().equals(o.getClass())) {
        return false;
      }

      Program program = (Program) o;
      return application.equals(program.application) && type.equals(program.type) && id.equals(program.id);
    }

    @Override
    public int hashCode() {
      int result = application.hashCode();
      result = 31 * result + id.hashCode();
      return result;
    }
  }

  /**
   * Uniquely identifies a Flowlet.
   */
  public static class Flowlet extends NamespacedId {

    private final Program flow;
    private final String id;

    private Flowlet(Program flow, String id) {
      Preconditions.checkArgument(flow != null, "flow cannot be null");
      Preconditions.checkArgument(flow.getType() == ProgramType.FLOW, "must be parented to a flow");
      Preconditions.checkArgument(id != null, "id cannot be null");
      this.flow = flow;
      this.id = id;
    }

    public static Flowlet from(Program flow, String id) {
      return new Flowlet(flow, id);
    }

    public static Flowlet from(Application app, String flowId, String id) {
      return new Flowlet(Id.Program.from(app, ProgramType.FLOW, flowId), id);
    }

    @Override
    public String toString() {
      return String.format("%s:%s.%s.%s.%s.%s", getIdType(), flow.getNamespaceId(), flow.getApplicationId(),
                           flow.getType().getPrettyName().toLowerCase(), flow.getId(), id);
    }

    @Override
    public Namespace getNamespace() {
      return flow.getNamespace();
    }

    public String getId() {
      return id;
    }

    public Program getFlow() {
      return flow;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Flowlet flowlet = (Flowlet) o;
      return Objects.equal(flow, flowlet.flow) &&
        Objects.equal(id, flowlet.id);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(flow, id);
    }

    /**
     * Uniquely identifies a Flowlet Queue.
     */
    public static final class Queue extends NamespacedId {

      private final Flowlet producer;
      private final String id;

      public Queue(Flowlet producer, String id) {
        this.producer = producer;
        this.id = id;
      }

      @Override
      public String toString() {
        return String.format("%s:%s.%s.%s.%s.%s", getIdType(),
                             producer.getFlow().getNamespaceId(),
                             producer.getFlow().getApplicationId(),
                             producer.getFlow().getType().getPrettyName().toLowerCase(),
                             producer.getId(), id);
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
    }
  }

  /**
   * Represents ID of a Schedule.
   */
  public static class Schedule extends NamespacedId {

    private final Application application;
    private final String id;

    private Schedule(Application application, String id) {
      Preconditions.checkArgument(application != null, "application cannot be null.");
      Preconditions.checkArgument(id != null && !id.isEmpty(), "id cannot be null or empty.");
      this.application = application;
      this.id = id;
    }

    public static Schedule from(Application application, String id) {
      return new Schedule(application, id);
    }

    public static Schedule from(Namespace namespace, String appId, String id) {
      return new Schedule(Id.Application.from(namespace, appId), id);
    }

    @Override
    public String toString() {
      return String.format("%s:%s.%s.%s", getIdType(),
                           application.getNamespaceId(),
                           application.getId(), id);
    }

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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Schedule that = (Schedule) o;
      return Objects.equal(application, that.application) &&
        Objects.equal(id, that.id);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(application, id);
    }
  }

  /**
   * Represents ID of a Notification feed.
   */
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

    private NotificationFeed(String namespace, String category, String name, String description) {
      Preconditions.checkArgument(namespace != null && !namespace.isEmpty(),
                                  "Namespace value cannot be null or empty.");
      Preconditions.checkArgument(category != null && !category.isEmpty(),
                                  "Category value cannot be null or empty.");
      Preconditions.checkArgument(name != null && !name.isEmpty(), "Name value cannot be null or empty.");
      Preconditions.checkArgument(isValidId(namespace) && isValidId(category) && isValidId(name),
                                  "Namespace, category or name has a wrong format.");

      this.namespace = Namespace.from(namespace);
      this.category = category;
      this.name = name;
      this.description = description;
    }

    @Override
    public String toString() {
      return String.format("%s:%s.%s.%s", getIdType(),
                           namespace.getId(),
                           category, name);
    }

    public String getCategory() {
      return category;
    }

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

    /**
     * Builder used to build {@link NotificationFeed}.
     */
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      NotificationFeed that = (NotificationFeed) o;
      return Objects.equal(this.namespace, that.namespace)
        && Objects.equal(this.category, that.category)
        && Objects.equal(this.name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(namespace, category, name);
    }
  }

  /**
   * Id.Stream uniquely identifies a stream.
   */
  public static final class Stream extends NamespacedId {
    private final Namespace namespace;
    private final String streamName;
    private transient int hashCode;

    private transient String id;
    private transient byte[] idBytes;

    private Stream(final Namespace namespace, final String streamName) {
      Preconditions.checkNotNull(namespace, "Namespace cannot be null.");
      Preconditions.checkNotNull(streamName, "Stream name cannot be null.");

      Preconditions.checkArgument(isValidId(streamName), "Stream name can only contain alphanumeric, " +
                                    "'-' and '_' characters: %s", streamName);

      this.namespace = namespace;
      this.streamName = streamName;
    }

    public static Stream from(Namespace id, String streamName) {
      return new Stream(id, streamName);
    }

    public static Stream from(String namespaceId, String streamName) {
      return from(Id.Namespace.from(namespaceId), streamName);
    }

    public static Stream fromId(String id) {
      Iterable<String> comps = Splitter.on('.').omitEmptyStrings().split(id);
      Preconditions.checkArgument(2 == Iterables.size(comps));

      String namespace = Iterables.get(comps, 0);
      String streamName = Iterables.get(comps, 1);
      return from(namespace, streamName);
    }

    @Override
    public String toString() {
      return String.format("%s:%s.%s", getIdType(),
                           namespace.getId(),
                           streamName);
    }

    @Override
    public Namespace getNamespace() {
      return namespace;
    }

    public String getId() {
      return streamName;
    }

    public String getNamespaceId() {
      return namespace.getId();
    }

    public String toId() {
      if (id == null) {
        id = String.format("%s.%s", namespace.getId(), streamName);
      }
      return id;
    }

    public byte[] toBytes() {
      if (idBytes == null) {
        idBytes = toId().getBytes(Charsets.US_ASCII);
      }
      return idBytes;
    }

    @Override
    public int hashCode() {
      int h = hashCode;
      if (h == 0) {
        h = 31 * namespace.hashCode() + streamName.hashCode();
        hashCode = h;
      }
      return h;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Stream that = (Stream) o;

      return this.namespace.equals(that.namespace) &&
        this.streamName.equals(that.streamName);
    }

    /**
     * Uniquely identifies a stream view.
     */
    public static final class View extends NamespacedId {
      private final Stream stream;
      private final String id;

      public View(Stream stream, String id) {
        Preconditions.checkNotNull(id, "ID cannot be null.");
        Preconditions.checkArgument(isValidId(id), "ID can only contain alphanumeric, " +
          "'-' and '_' characters: %s", id);
        this.stream = stream;
        this.id = id;
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
      public String toString() {
        return String.format("%s:%s.%s.%s", getIdType(),
                             stream.getNamespaceId(),
                             stream.getId(),
                             id);
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
        View view = (View) o;
        return java.util.Objects.equals(stream, view.stream) &&
          java.util.Objects.equals(id, view.id);
      }

      @Override
      public int hashCode() {
        return java.util.Objects.hash(stream, id);
      }
    }
  }

  /**
   * Dataset Type Id identifies a given dataset module.
   */
  public static final class DatasetType extends NamespacedId {
    private final Namespace namespace;
    private final String typeName;

    private DatasetType(Namespace namespace, String typeName) {
      Preconditions.checkNotNull(namespace, "Namespace cannot be null.");
      Preconditions.checkNotNull(typeName, "Dataset type id cannot be null.");
      Preconditions.checkArgument(isValidDatasetId(typeName), "Invalid characters found in dataset type Id. '" +
        typeName + "'. Module id can contain alphabets, numbers or _, -, . or $ characters");
      this.namespace = namespace;
      this.typeName = typeName;
    }

    public static DatasetType from(Namespace id, String typeId) {
      return new DatasetType(id, typeId);
    }

    public static DatasetType from(String namespaceId, String typeId) {
      return new DatasetType(Namespace.from(namespaceId), typeId);
    }

    @Override
    public String toString() {
      return String.format("%s:%s.%s", getIdType(), namespace.getId(), typeName);
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      DatasetType that = (DatasetType) o;
      return namespace.equals(that.namespace) && typeName.equals(that.typeName);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(namespace, typeName);
    }

    public String getId() {
      return typeName;
    }
  }

  /**
   * Dataset Module Id identifies a given dataset module.
   */
  public static final class DatasetModule extends NamespacedId {
    private final Namespace namespace;
    private final String moduleId;

    private DatasetModule(Namespace namespace, String moduleId) {
      Preconditions.checkNotNull(namespace, "Namespace cannot be null.");
      Preconditions.checkNotNull(moduleId, "Dataset module id cannot be null.");
      Preconditions.checkArgument(isValidDatasetId(moduleId), "Invalid characters found in dataset module Id. '" +
        moduleId + "'. Module id can contain alphabets, numbers or _, -, . or $ characters");
      this.namespace = namespace;
      this.moduleId = moduleId;
    }

    public static DatasetModule from(Namespace id, String moduleId) {
      return new DatasetModule(id, moduleId);
    }

    public static DatasetModule from(String namespaceId, String moduleId) {
      return new DatasetModule(Namespace.from(namespaceId), moduleId);
    }

    @Override
    public String toString() {
      return String.format("%s:%s.%s", getIdType(), namespace.getId(), moduleId);
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      DatasetModule that = (DatasetModule) o;
      return namespace.equals(that.namespace) && moduleId.equals(that.moduleId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(namespace, moduleId);
    }
  }

  /**
   * Dataset Instance Id identifies a given dataset instance.
   */
  public static final class DatasetInstance extends NamespacedId {
    private final Namespace namespace;
    private final String instanceId;

    private DatasetInstance(Namespace namespace, String instanceId) {
      Preconditions.checkNotNull(namespace, "Namespace cannot be null.");
      Preconditions.checkNotNull(instanceId, "Dataset instance id cannot be null.");
      Preconditions.checkArgument(isValidDatasetId(instanceId), "Invalid characters found in dataset instance id. '" +
        instanceId + "'. Instance id can contain alphabets, numbers or _, -, . or $ characters");
      this.namespace = namespace;
      this.instanceId = instanceId;
    }

    public static DatasetInstance from(Namespace id, String instanceId) {
      return new DatasetInstance(id, instanceId);
    }

    public static DatasetInstance from(String namespaceId, String instanceId) {
      return new DatasetInstance(Namespace.from(namespaceId), instanceId);
    }

    @Override
    public String toString() {
      return String.format("%s:%s.%s", getIdType(), namespace.getId(), instanceId);
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      DatasetInstance that = (DatasetInstance) o;
      return namespace.equals(that.namespace) && instanceId.equals(that.instanceId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(namespace, instanceId);
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
      Preconditions.checkNotNull(namespace, "Namespace cannot be null.");
      Preconditions.checkNotNull(name, "Name cannot be null.");
      Preconditions.checkArgument(isValidId(name), "Invalid artifact name.");
      Preconditions.checkNotNull(version, "Version cannot be null.");
      Preconditions.checkNotNull(version.getVersion(), "Invalid artifact version.");
      this.namespace = namespace;
      this.name = name;
      this.version = version;
    }

    public static Artifact from(Namespace namespace, String name, String version) {
      return new Artifact(namespace, name, new ArtifactVersion(version));
    }

    public static Artifact from(Namespace namespace, String name, ArtifactVersion version) {
      return new Artifact(namespace, name, version);
    }

    public static Artifact from(Id.Namespace namespace, ArtifactId id) {
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
    public String toString() {
      return String.format("%s:%s.%s.%s", getIdType(), namespace.getId(), name, version.getVersion());
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

    public String getId() {
      return String.format("%s-%s", name, version.getVersion());
    }

    public ArtifactId toArtifactId() {
      return new ArtifactId(name, version,
                            Namespace.SYSTEM.equals(namespace) ? ArtifactScope.SYSTEM : ArtifactScope.USER);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Artifact that = (Artifact) o;

      return this.compareTo(that) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(namespace, name, version);
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
  }

}
