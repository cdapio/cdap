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

import co.cask.cdap.api.schedule.SchedulableProgramType;
import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;

/**
 * Contains collection of classes representing different types of Ids.
 */
public abstract class Id {

  private static boolean isId(String name) {
    return CharMatcher.inRange('A', 'Z')
      .or(CharMatcher.inRange('a', 'z'))
      .or(CharMatcher.is('-'))
      .or(CharMatcher.is('_'))
      .or(CharMatcher.inRange('0', '9')).matchesAllOf(name);
  }

  /**
   * Allow '.' and '$' for dataset ids since they can be fully qualified class names
   */
  private static boolean isValidDatasetId(String datasetId) {
    return CharMatcher.inRange('A', 'Z')
      .or(CharMatcher.inRange('a', 'z'))
      .or(CharMatcher.is('-'))
      .or(CharMatcher.is('_'))
      .or(CharMatcher.inRange('0', '9'))
      .or(CharMatcher.is('.'))
      .or(CharMatcher.is('$')).matchesAllOf(datasetId);
  }

  public final String getIdType() {
    // TODO: refactor
    return this.getClass().getSimpleName().toLowerCase();
  }

  public final String getIdRep() {
    Id parent = getParent();
    if (parent == null) {
      return getIdType() + ":" + getId();
    } else {
      return parent.getIdRep() + "/" + getIdType() + ":" + getId();
    }
  }

  @Nullable
  protected abstract Id getParent();

  public abstract String getId();

  /**
   * Placeholder for no id.
   */
  public static final class None extends Id {

    public None() {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    protected Id getParent() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getId() {
      throw new UnsupportedOperationException();
    }
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

    @Nullable
    @Override
    protected Id getParent() {
      return null;
    }

    @Override
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

    @Nullable
    @Override
    protected Id getParent() {
      return null;
    }

    @Override
    public String getId() {
      return id;
    }
  }

  /**
   * Uniquely identifies a Namespace.
   */
  public static final class Namespace extends Id {
    public static final Namespace DEFAULT = Id.Namespace.from("default");

    private final String id;

    public Namespace(String id) {
      Preconditions.checkNotNull(id, "Namespace '" + id + "' cannot be null.");
      Preconditions.checkArgument(isId(id), "Namespace '" + id + "' has an incorrect format.");
      this.id = id;
    }

    @Override
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

    @Override
    public String toString() {
      return id;
    }

    @Nullable
    @Override
    protected Id getParent() {
      return null;
    }
  }

  /**
   * Uniquely identifies an Adapter Type.
   */
  public static final class AdapterType extends Id {
    private final String adapterTypeId;

    public AdapterType(final String adapterTypeId) {
      Preconditions.checkNotNull(adapterTypeId, "adapterTypeId cannot be null.");
      this.adapterTypeId = adapterTypeId;
    }

    @Override
    public String getId() {
      return adapterTypeId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      AdapterType that = (AdapterType) o;
      return adapterTypeId.equals(that.adapterTypeId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(adapterTypeId);
    }

    public static AdapterType from(String adapterTypeId) {
      return new AdapterType(adapterTypeId);
    }

    @Override
    public Id getParent() {
      return null;
    }
  }

  /**
   * Uniquely identifies an Application.
   */
  public static final class Application extends NamespacedId {
    private final Namespace namespace;
    private final String applicationId;

    public Application(final Namespace namespace, final String applicationId) {
      Preconditions.checkNotNull(namespace, "namespace cannot be null.");
      Preconditions.checkNotNull(applicationId, "applicationId cannot be null.");
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

    public static Application from(Namespace id, String applicationId) {
      return new Application(id, applicationId);
    }

    public static Application from(String namespaceId, String applicationId) {
      return new Application(Namespace.from(namespaceId), applicationId);
    }

    public static Application from(Adapter adapter, String adapterSpecType) {
      return new Application(adapter.getNamespace(), adapterSpecType);
    }

    @Override
    public Id getParent() {
      return namespace;
    }
  }

  /**
   * Uniquely identifies an Adapter.
   */
  public static final class Adapter extends NamespacedId {
    private final Namespace namespace;
    private final String adapterId;

    public Adapter(final Namespace namespace, final String adapterId) {
      Preconditions.checkNotNull(namespace, "namespace cannot be null.");
      Preconditions.checkNotNull(adapterId, "adapterId cannot be null.");
      this.namespace = namespace;
      this.adapterId = adapterId;
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
      return adapterId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Adapter that = (Adapter) o;
      return namespace.equals(that.namespace) && adapterId.equals(that.adapterId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(namespace, adapterId);
    }

    public static Adapter from(Namespace id, String adapterId) {
      return new Adapter(id, adapterId);
    }

    public static Adapter from(String namespaceId, String adapterId) {
      return new Adapter(Namespace.from(namespaceId), adapterId);
    }

    @Override
    public Id getParent() {
      return namespace;
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

    @Override
    public Id getParent() {
      return application;
    }

    /**
     * Uniquely identifies a Program's run record.
     */
    public static class RunRecord extends NamespacedId {

      private final Program program;
      private final String pid;

      public RunRecord(Program program, String pid) {
        this.program = program;
        this.pid = pid;
      }

      @Override
      public Namespace getNamespace() {
        return program.getNamespace();
      }

      @Nullable
      @Override
      protected Id getParent() {
        return program;
      }

      @Override
      public String getId() {
        return pid;
      }

      public Program getProgram() {
        return program;
      }
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

    /**
     * Uniquely identifies a Service Runnable.
     */
    public static class Runnable extends NamespacedId {

      private final Service service;
      private final String id;

      private Runnable(Service service, String id) {
        Preconditions.checkArgument(service != null, "flow cannot be null");
        Preconditions.checkArgument(id != null, "id cannot be null");
        this.service = service;
        this.id = id;
      }

      public static Runnable from(Service service, String id) {
        return new Runnable(service, id);
      }

      @Override
      public Namespace getNamespace() {
        return service.getNamespace();
      }

      @Nullable
      @Override
      protected Id getParent() {
        return service;
      }

      @Override
      public String getId() {
        return id;
      }
    }
  }

  /**
   * Uniquely identifies a Flow.
   */
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

    /**
     * Uniquely identifies a Flowlet.
     */
    public static class Flowlet extends NamespacedId {

      private final Flow flow;
      private final String id;

      private Flowlet(Flow flow, String id) {
        Preconditions.checkArgument(flow != null, "flow cannot be null");
        Preconditions.checkArgument(id != null, "id cannot be null");
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

      @Nullable
      @Override
      protected Id getParent() {
        return flow;
      }

      @Override
      public String getId() {
        return id;
      }
    }
  }

  /**
   * Uniquely identifies a Procedure.
   */
  public static class Procedure extends Program {

    private Procedure(Application application, String id) {
      super(application, ProgramType.PROCEDURE, id);
    }

    public static Procedure from(Application application, String id) {
      return new Procedure(application, id);
    }

    public static Procedure from(String appId, String procedureId) {
      return new Procedure(Id.Application.from(Namespace.DEFAULT, appId), procedureId);
    }

    /**
     * Uniquely identifies a Procedure Handler.
     */
    public static class Method extends NamespacedId {

      private final Procedure procedure;
      private final String id;

      private Method(Procedure procedure, String id) {
        Preconditions.checkArgument(procedure != null, "procedure cannot be null");
        Preconditions.checkArgument(id != null, "id cannot be null");
        this.procedure = procedure;
        this.id = id;
      }

      public static Method from(Procedure procedure, String id) {
        return new Method(procedure, id);
      }

      @Override
      public Namespace getNamespace() {
        return procedure.getNamespace();
      }

      @Nullable
      @Override
      protected Id getParent() {
        return procedure;
      }

      @Override
      public String getId() {
        return id;
      }
    }
  }

  /**
   * Represents ID of a Schedule.
   */
  public static class Schedule extends NamespacedId {

    private final Program program;
    private final SchedulableProgramType schedulableProgramType;
    private final String id;

    private Schedule(Program program, SchedulableProgramType schedulableProgramType, String id) {
      Preconditions.checkArgument(program != null, "program cannot be null.");
      Preconditions.checkArgument(schedulableProgramType != null, "schedulableProgramType cannot be null.");
      Preconditions.checkArgument(id != null && !id.isEmpty(), "id cannot be null or empty.");
      this.program = program;
      this.schedulableProgramType = schedulableProgramType;
      this.id = id;
    }

    public Program getProgram() {
      return program;
    }

    public SchedulableProgramType getSchedulableProgramType() {
      return schedulableProgramType;
    }

    @Override
    public Id getParent() {
      return program;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public Namespace getNamespace() {
      return program.getNamespace();
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("program", program)
        .add("schedulableProgramType", schedulableProgramType)
        .add("id", id).toString();
    }

    public static Schedule from(Program program, SchedulableProgramType schedulableProgramType, String id) {
      return new Schedule(program, schedulableProgramType, id);
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
      Preconditions.checkArgument(isId(namespace) && isId(category) && isId(name),
                                  "Namespace, category or name has a wrong format.");

      this.namespace = Namespace.from(namespace);
      this.category = category;
      this.name = name;
      this.description = description;
    }

    public String getCategory() {
      return category;
    }

    @Nullable
    @Override
    protected Id getParent() {
      return namespace;
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
    public String toString() {
      return Objects.toStringHelper(this)
        .add("namespace", namespace)
        .add("category", category)
        .add("name", name)
        .add("description", description)
        .toString();
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

    private Stream(final String namespace, final String streamName) {
      Preconditions.checkNotNull(namespace, "Namespace cannot be null.");
      Preconditions.checkNotNull(streamName, "Stream name cannot be null.");

      Preconditions.checkArgument(isId(namespace), "Stream namespace has an incorrect format.");
      Preconditions.checkArgument(isId(streamName),
                                  "Stream name can only contains alphanumeric, '-' and '_' characters only.");

      this.namespace = Id.Namespace.from(namespace);
      this.streamName = streamName;
    }

    @Override
    public Namespace getNamespace() {
      return namespace;
    }

    @Nullable
    @Override
    protected Id getParent() {
      return namespace;
    }

    @Override
    public String getId() {
      return streamName;
    }

    public String getNamespaceId() {
      return namespace.getId();
    }

    public static Stream from(Namespace id, String streamName) {
      return new Stream(id.getId(), streamName);
    }

    public static Stream from(String namespaceId, String streamName) {
      return new Stream(namespaceId, streamName);
    }

    public static Stream fromId(String id) {
      Iterable<String> comps = Splitter.on('.').omitEmptyStrings().split(id);
      Preconditions.checkArgument(2 == Iterables.size(comps));

      String namespace = Iterables.get(comps, 0);
      String streamName = Iterables.get(comps, 1);
      return from(namespace, streamName);
    }

    public String toId() {
      if (id == null) {
        id = String.format("%s.%s", namespace, streamName);
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

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("namespace", namespace)
        .add("streamName", streamName)
        .toString();
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

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("namespace", namespace)
        .add("typeName", typeName)
        .toString();
    }

    public static DatasetType from(Namespace id, String typeId) {
      return new DatasetType(id, typeId);
    }

    public static DatasetType from(String namespaceId, String typeId) {
      return new DatasetType(Namespace.from(namespaceId), typeId);
    }

    @Nullable
    @Override
    protected Id getParent() {
      return namespace;
    }

    @Override
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

    @Override
    public Namespace getNamespace() {
      return namespace;
    }

    public String getNamespaceId() {
      return namespace.getId();
    }

    @Nullable
    @Override
    protected Id getParent() {
      return namespace;
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

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
       .add("namespace", namespace)
       .add("module", moduleId)
       .toString();
    }

    public static DatasetModule from(Namespace id, String moduleId) {
      return new DatasetModule(id, moduleId);
    }

    public static DatasetModule from(String namespaceId, String moduleId) {
      return new DatasetModule(Namespace.from(namespaceId), moduleId);
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

    @Override
    public Namespace getNamespace() {
      return namespace;
    }

    public String getNamespaceId() {
      return namespace.getId();
    }

    @Nullable
    @Override
    protected Id getParent() {
      return namespace;
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

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("namespace", namespace)
        .add("instance", instanceId)
        .toString();
    }

    public static DatasetInstance from(Namespace id, String instanceId) {
      return new DatasetInstance(id, instanceId);
    }

    public static DatasetInstance from(String namespaceId, String instanceId) {
      return new DatasetInstance(Namespace.from(namespaceId), instanceId);
    }
  }
}
