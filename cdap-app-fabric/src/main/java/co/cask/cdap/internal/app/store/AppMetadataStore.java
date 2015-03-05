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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.common.exception.AlreadyExistsException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.internal.app.runtime.adapter.AdapterStatus;
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Store for application metadata
 */
public class AppMetadataStore extends MetadataStoreDataset {
  private static final Logger LOG = LoggerFactory.getLogger(AppMetadataStore.class);

  private static final Gson GSON;

  static {
    GsonBuilder builder = new GsonBuilder();
    ApplicationSpecificationAdapter.addTypeAdapters(builder);
    GSON = builder.create();
  }

  private static final String TYPE_APP_META = "appMeta";
  private static final String TYPE_PROGRAM = "program";
  private static final String TYPE_STREAM = "stream";
  private static final String TYPE_RUN_RECORD_STARTED = "runRecordStarted";
  private static final String TYPE_RUN_RECORD_COMPLETED = "runRecordCompleted";
  private static final String TYPE_PROGRAM_ARGS = "programArgs";
  private static final String TYPE_NAMESPACE = "namespace";
  private static final String TYPE_ADAPTER = "adapter";

  private final RootView root;
  private final NamespaceView namespaces;
  private final StreamView streams;
  private final AdapterView adapters;
  private final AppView apps;

  private final ProgramView programs;
  private final RunRecordView runsStarted;
  private final RunRecordView runsCompleted;
  private final ProgramArgsView programArgs;

  public AppMetadataStore(Table table) {
    super(table);
    this.root = new RootView(this);
    this.namespaces = new NamespaceView(root, this, TYPE_NAMESPACE);
    this.apps = new AppView(namespaces, this, TYPE_APP_META);
    this.programs = new ProgramView(apps, this, TYPE_PROGRAM);
    this.programArgs = new ProgramArgsView(programs, this, TYPE_PROGRAM_ARGS);
    this.runsStarted = new RunRecordView(programs, this, TYPE_RUN_RECORD_STARTED);
    this.runsCompleted = new RunRecordView(programs, this, TYPE_RUN_RECORD_COMPLETED);
    this.streams = new StreamView(namespaces, this, TYPE_STREAM);
    this.adapters = new AdapterView(namespaces, this, TYPE_ADAPTER);
  }

  @Override
  protected <T> byte[] serialize(T value) {
    return Bytes.toBytes(GSON.toJson(value));
  }

  @Override
  protected <T> T deserialize(byte[] serialized, Class<T> classOfT) {
    return GSON.fromJson(Bytes.toString(serialized), classOfT);
  }

  public ApplicationMeta getApplication(Id.Application application) throws NotFoundException {
    return apps.get(application);
  }

  public ApplicationMeta getApplication(String namespaceId, String appId) throws NotFoundException {
    return apps.get(Id.Application.from(namespaceId, appId));
  }

  public List<ApplicationMeta> getAllApplications(String namespaceId) throws NotFoundException {
    return apps.list(Id.Namespace.from(namespaceId));
  }

  public void writeApplication(String namespaceId, String appId, ApplicationSpecification spec,
                               String archiveLocation) throws AlreadyExistsException, NotFoundException {
    // TODO: throw not found

    // NOTE: we use Gson underneath to do serde, as it doesn't serialize inner classes (which we use everywhere for
    //       specs - see forwarding specs), we want to wrap spec with DefaultApplicationSpecification
    spec = DefaultApplicationSpecification.from(spec);
    apps.create(Id.Application.from(namespaceId, appId), new ApplicationMeta(appId, spec, archiveLocation));
  }

  public void deleteApplication(String namespaceId, String appId) throws NotFoundException {
    apps.delete(Id.Application.from(namespaceId, appId));
  }

  public void deleteApplications(String namespaceId) throws NotFoundException {
    apps.deleteAll(Id.Namespace.from(namespaceId));
  }

  // todo: do we need appId? may be use from appSpec?
  public void updateAppSpec(String namespaceId, String appId, ApplicationSpecification spec) {
    // NOTE: we use Gson underneath to do serde, as it doesn't serialize inner classes (which we use everywhere for
    //       specs - see forwarding specs), we want to wrap spec with DefaultApplicationSpecification
    spec = DefaultApplicationSpecification.from(spec);
    LOG.trace("App spec to be updated: id: {}: spec: {}", appId, GSON.toJson(spec));
    MDSKey key = new MDSKey.Builder().add(TYPE_APP_META, namespaceId, appId).build();
    ApplicationMeta existing = get(key, ApplicationMeta.class);
    if (existing == null) {
      String msg = String.format("No meta for namespace %s app %s exists", namespaceId, appId);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    LOG.trace("Application exists in mds: id: {}, spec: {}", existing);
    ApplicationMeta updated = ApplicationMeta.updateSpec(existing, spec);
    write(key, updated);

    for (StreamSpecification stream : spec.getStreams().values()) {
      writeStream(namespaceId, stream);
    }
  }

  public void recordProgramStart(Id.Program program, String pid, long startTs) throws NotFoundException {
    // TODO: throw not found
      write(new MDSKey.Builder().add(TYPE_RUN_RECORD_STARTED)
              .add(program.getNamespaceId())
              .add(program.getApplicationId())
              .add(program.getType().name())
              .add(program.getId())
              .add(pid)
              .build(),
            new RunRecord(pid, startTs, null, ProgramRunStatus.RUNNING));
  }

  public void recordProgramStop(Id.Program program, String pid, long stopTs,
                                ProgramController.State endStatus) throws NotFoundException {
    // TODO: throw not found

    MDSKey key = new MDSKey.Builder()
      .add(TYPE_RUN_RECORD_STARTED)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(program.getType().name())
      .add(program.getId())
      .add(pid)
      .build();
    RunRecord started = get(key, RunRecord.class);
    if (started == null) {
      String msg = String.format("No meta for started run record for namespace %s app %s program type %s " +
                                 "program %s pid %s exists",
                                 program.getNamespaceId(), program.getApplicationId(), program.getType().name(),
                                 program.getId(), pid);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    deleteAll(key);

    key = new MDSKey.Builder()
      .add(TYPE_RUN_RECORD_COMPLETED)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(program.getType().name())
      .add(program.getId())
      .add(getInvertedTsKeyPart(started.getStartTs()))
      .add(pid).build();
    write(key, new RunRecord(started, stopTs, endStatus.getRunStatus()));
  }

  public List<RunRecord> getRuns(Id.Program program, ProgramRunStatus status,
                                 long startTime, long endTime, int limit) throws NotFoundException {
    // TODO: throw not found
    if (status.equals(ProgramRunStatus.ALL)) {
      List<RunRecord> resultRecords = Lists.newArrayList();
      resultRecords.addAll(getActiveRuns(program, startTime, endTime, limit));
      resultRecords.addAll(getHistoricalRuns(program, status, startTime, endTime, limit));
      return resultRecords;
    } else if (status.equals(ProgramRunStatus.RUNNING)) {
      return getActiveRuns(program, startTime, endTime, limit);
    } else {
      return getHistoricalRuns(program, status, startTime, endTime, limit);
    }
  }

  private List<RunRecord> getActiveRuns(Id.Program program, final long startTime, final long endTime, int limit) {
    MDSKey activeKey = new MDSKey.Builder()
      .add(TYPE_RUN_RECORD_STARTED)
      .add(program.getNamespaceId())
      .add(program.getApplicationId())
      .add(program.getType().name())
      .add(program.getId())
      .build();
    MDSKey start = new MDSKey.Builder(activeKey).add(getInvertedTsKeyPart(endTime)).build();
    MDSKey stop = new MDSKey.Builder(activeKey).add(getInvertedTsKeyPart(startTime)).build();
    return list(start, stop, RunRecord.class, limit, Predicates.<RunRecord>alwaysTrue());
  }

  private List<RunRecord> getHistoricalRuns(Id.Program program, ProgramRunStatus status,
                                            final long startTime, final long endTime, int limit) {
    MDSKey historyKey = new MDSKey.Builder().add(TYPE_RUN_RECORD_COMPLETED,
                                                 program.getNamespaceId(),
                                                 program.getApplicationId(),
                                                 program.getType().name(),
                                                 program.getId()).build();
    MDSKey start = new MDSKey.Builder(historyKey).add(getInvertedTsKeyPart(endTime)).build();
    MDSKey stop = new MDSKey.Builder(historyKey).add(getInvertedTsKeyPart(startTime)).build();
    if (status.equals(ProgramRunStatus.ALL)) {
      //return all records (successful and failed)
      return list(start, stop, RunRecord.class, limit, Predicates.<RunRecord>alwaysTrue());
    }
    if (status.equals(ProgramRunStatus.COMPLETED)) {
      return list(start, stop, RunRecord.class, limit, getPredicate(ProgramController.State.STOPPED));
    }
    return list(start, stop, RunRecord.class, limit, getPredicate(ProgramController.State.ERROR));
  }

  private Predicate<RunRecord> getPredicate(final ProgramController.State state) {
    return new Predicate<RunRecord>() {
      @Override
      public boolean apply(RunRecord record) {
        return record.getStatus().equals(state.getRunStatus());
      }
    };
  }

  private long getInvertedTsKeyPart(long endTime) {
    return Long.MAX_VALUE - endTime;
  }

  public void writeStream(String namespaceId, StreamSpecification spec) {
    // TODO: throw namespace not found
    write(new MDSKey.Builder().add(TYPE_STREAM, namespaceId, spec.getName()).build(), spec);
  }

  public StreamSpecification getStream(String namespaceId, String name) {
    return get(new MDSKey.Builder().add(TYPE_STREAM, namespaceId, name).build(), StreamSpecification.class);
  }

  public List<StreamSpecification> getAllStreams(String namespaceId) {
    // TODO: throw namespace not found
    return list(new MDSKey.Builder().add(TYPE_STREAM, namespaceId).build(), StreamSpecification.class);
  }

  public void deleteStreams(String namespaceId) {
    deleteAll(new MDSKey.Builder().add(TYPE_STREAM, namespaceId).build());
  }

  public void deleteStream(String namespaceId, String name) {
    deleteAll(new MDSKey.Builder().add(TYPE_STREAM, namespaceId, name).build());
  }

  public void writeProgramArgs(Id.Program program, Map<String, String> args) {
    write(new MDSKey.Builder()
            .add(TYPE_PROGRAM_ARGS)
            .add(program.getNamespaceId())
            .add(program.getApplicationId())
            .add(program.getType().name())
            .add(program.getId())
            .build(), new ProgramArgs(args));
  }

  public ProgramArgs getProgramArgs(Id.Program program) {
    return get(new MDSKey.Builder()
                 .add(TYPE_PROGRAM_ARGS)
                 .add(program.getNamespaceId())
                 .add(program.getApplicationId())
                 .add(program.getType().name())
                 .add(program.getId())
                 .build(), ProgramArgs.class);
  }

  public void deleteProgramArgs(Id.Program program) throws NotFoundException {
    programArgs.delete(program);
  }

  public void createNamespace(NamespaceMeta metadata) throws AlreadyExistsException {
    namespaces.create(Id.Namespace.from(metadata.getId()), metadata);
  }

  public NamespaceMeta getNamespace(Id.Namespace id) throws NotFoundException {
    return namespaces.get(id);
  }

  public void deleteNamespace(Id.Namespace id) throws NotFoundException {
    namespaces.deleteAll(null);
  }

  public List<NamespaceMeta> listNamespaces() throws NotFoundException {
    return namespaces.list(null);
  }

  public void writeAdapter(Id.Namespace id, AdapterSpecification adapterSpec,
                           AdapterStatus adapterStatus) throws AlreadyExistsException {
    Id.Adapter adapter = Id.Adapter.from(id, adapterSpec.getName());
    adapters.create(adapter, new AdapterMeta(adapterSpec, adapterStatus));
  }

  public AdapterSpecification getAdapter(Id.Adapter adapter) throws NotFoundException {
    return adapters.get(adapter).getSpec();
  }

  public AdapterStatus getAdapterStatus(Id.Adapter adapter) throws NotFoundException {
    return adapters.get(adapter).getStatus();
  }

  public AdapterStatus setAdapterStatus(Id.Adapter adapter, AdapterStatus status) throws NotFoundException {
    AdapterMeta adapterMeta = adapters.get(adapter);
    AdapterStatus previousStatus = adapterMeta.getStatus();
    adapters.update(adapter, new AdapterMeta(adapterMeta.getSpec(), status));
    return previousStatus;
  }

  private AdapterMeta getAdapterMeta(Id.Adapter adapter) throws NotFoundException {
    return adapters.get(adapter);
  }

  public List<AdapterSpecification> getAllAdapters(Id.Namespace id) throws NotFoundException {
    List<AdapterMeta> adapterMetas = adapters.list(id);
    List<AdapterSpecification> adapterSpecs = Lists.newArrayList();
    for (AdapterMeta adapterMeta : adapterMetas) {
      adapterSpecs.add(adapterMeta.getSpec());
    }
    return adapterSpecs;
  }

  public void deleteAdapter(Id.Adapter adapter) throws NotFoundException {
    adapters.delete(adapter);
  }

  public void deleteAdapters(Id.Namespace id) throws NotFoundException {
    adapters.deleteAll(id);
  }

  // ---------------------------------------------------------------------------

  public static final class RootView extends MetadataStoreView<Id.None, Void> {
    public RootView(MetadataStoreDataset metadataStore) {
      super(metadataStore, null, null);
    }

    @Override
    protected void appendKey(MDSKey.Builder builder, Id.None id) {
      // NO-OP
    }
  }

  public static final class NamespaceView extends ParentedMetadataStoreView<Id.None, Id.Namespace, NamespaceMeta> {
    public NamespaceView(RootView rootView, MetadataStoreDataset metadataStore, String partition) {
      super(metadataStore, NamespaceMeta.class, partition, rootView);
    }

    @Override
    protected void appendKey(MDSKey.Builder builder, Id.Namespace id) {
      builder.add(id.getId());
    }

    @Override
    protected Id.None getParent(Id.Namespace id) {
      throw new UnsupportedOperationException();
    }
  }

  public static final class AppView extends ParentedMetadataStoreView<Id.Namespace, Id.Application, ApplicationMeta> {
    public AppView(NamespaceView namespaceView, MetadataStoreDataset metadataStore, String partition) {
      super(metadataStore, ApplicationMeta.class, partition, namespaceView);
    }

    @Override
    protected Id.Namespace getParent(Id.Application id) {
      return id.getNamespace();
    }

    @Override
    protected void appendKey(MDSKey.Builder builder, Id.Application id) {
      builder.add(id.getId());
    }
  }

  public static final class ProgramView
    extends ParentedMetadataStoreView<Id.Application, Id.Application.Program, ProgramSpecification> {

    public ProgramView(AppView appView, MetadataStoreDataset metadataStore, String partition) {
      super(metadataStore, ProgramSpecification.class, partition, appView);
    }

    @Override
    protected Id.Application getParent(Id.Application.Program id) {
      return id.getApplication();
    }

    @Override
    protected void appendKey(MDSKey.Builder builder, Id.Application.Program id) {
      builder.add(id.getId());
    }
  }

  public static final class AdapterView extends ParentedMetadataStoreView<Id.Namespace, Id.Adapter, AdapterMeta> {
    public AdapterView(NamespaceView namespaceView, MetadataStoreDataset metadataStore, String partition) {
      super(metadataStore, AdapterMeta.class, partition, namespaceView);
    }

    @Override
    protected Id.Namespace getParent(Id.Adapter id) {
      return id.getNamespace();
    }

    @Override
    protected void appendKey(MDSKey.Builder builder, Id.Adapter id) {
      builder.add(id.getId());
    }
  }

//  private static final class ProgramView extends MetadataStoreView<Id.Application, Id.Program, ApplicationMeta> {
//    public ProgramView(AppView apps, MetadataStoreDataset metadataStore, String partition) {
//      super(metadataStore, ProgramMeta.class, partition, apps);
//    }
//
//    @Override
//    protected Id.Namespace getParent(Id.Application id) {
//      return id.getNamespace();
//    }
//
//    @Override
//    protected void appendKey(MDSKey.Builder builder, Id.Application id) {
//      builder.add(id.getId());
//    }
//  }

  public static final class StreamView extends ParentedMetadataStoreView<Id.Namespace, Id.Stream, StreamSpecification> {
    public StreamView(NamespaceView namespaceView, MetadataStoreDataset metadataStore, String partition) {
      super(metadataStore, StreamSpecification.class, partition, namespaceView);
    }

    @Override
    protected Id.Namespace getParent(Id.Stream id) {
      return id.getNamespace();
    }

    @Override
    protected void appendKey(MDSKey.Builder builder, Id.Stream id) {
      builder.add(id.getId());
    }
  }

  public static final class RunRecordView
    extends ParentedMetadataStoreView<Id.Program, Id.Program.RunRecord, RunRecord> {

    public RunRecordView(ProgramView programView, MetadataStoreDataset metadataStore, String partition) {
      super(metadataStore, RunRecord.class, partition, programView);
    }

    @Override
    protected Id.Program getParent(Id.Program.RunRecord id) {
      return id.getProgram();
    }

    @Override
    protected void appendKey(MDSKey.Builder builder, Id.Program.RunRecord id) {
      builder.add(id.getId());
    }
  }

  public static final class ProgramArgsView
    extends ParentedMetadataStoreView<Id.Program, Id.Program, ProgramArgs> {

    public ProgramArgsView(ProgramView programView, MetadataStoreDataset metadataStore, String partition) {
      super(metadataStore, ProgramArgs.class, partition, programView);
    }

    @Override
    protected Id.Program getParent(Id.Program id) {
      return id;
    }

    @Override
    protected void appendKey(MDSKey.Builder builder, Id.Program id) {
      // NO-OP
    }
  }
}
