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

package co.cask.cdap.gateway.handlers.util;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.procedure.ProcedureSpecification;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.services.Data;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.DatasetRecord;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.StreamRecord;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Abstract Class that contains commonly used methods for parsing Http Requests.
 */
public abstract class AbstractAppFabricHttpHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAppFabricHttpHandler.class);

  /**
   * Json serializer.
   */
  private static final Gson GSON = new Gson();

  protected static final java.lang.reflect.Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  /**
   * Name of the header that should specify the application archive
   */
  public static final String ARCHIVE_NAME_HEADER = "X-Archive-Name";

  /**
   * Class to represent status of programs.
   */
  protected static final class AppFabricServiceStatus {

    public static final AppFabricServiceStatus OK = new AppFabricServiceStatus(HttpResponseStatus.OK, "");

    public static final AppFabricServiceStatus PROGRAM_STILL_RUNNING =
      new AppFabricServiceStatus(HttpResponseStatus.FORBIDDEN, "Program is still running");

    public static final AppFabricServiceStatus PROGRAM_ALREADY_RUNNING =
      new AppFabricServiceStatus(HttpResponseStatus.CONFLICT, "Program is already running");

    public static final AppFabricServiceStatus PROGRAM_ALREADY_STOPPED =
      new AppFabricServiceStatus(HttpResponseStatus.CONFLICT, "Program already stopped");

    public static final AppFabricServiceStatus RUNTIME_INFO_NOT_FOUND =
      new AppFabricServiceStatus(HttpResponseStatus.CONFLICT,
                                 UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND));

    public static final AppFabricServiceStatus PROGRAM_NOT_FOUND =
      new AppFabricServiceStatus(HttpResponseStatus.NOT_FOUND, "Program not found");

    public static final AppFabricServiceStatus INTERNAL_ERROR =
      new AppFabricServiceStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal server error");

    private final HttpResponseStatus code;
    private final String message;

    /**
     * Describes the output status of app fabric operations.
     */
    public AppFabricServiceStatus(HttpResponseStatus code, String message) {
      this.code = code;
      this.message = message;
    }

    public HttpResponseStatus getCode() {
      return code;
    }

    public String getMessage() {
      return message;
    }
  }

  public AbstractAppFabricHttpHandler(Authenticator authenticator) {
    super(authenticator);
  }

  protected int getInstances(HttpRequest request) throws IOException, NumberFormatException {
    return parseBody(request, Instances.class).getInstances();
  }

  @Nullable
  protected <T> T parseBody(HttpRequest request, Class<T> type) throws IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      return null;
    }
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      return GSON.fromJson(reader, type);
    } catch (JsonSyntaxException e) {
      LOG.info("Failed to parse body on {} as {}", request.getUri(), type, e);
      throw e;
    } finally {
      reader.close();
    }
  }

  protected Map<String, String> decodeArguments(HttpRequest request) throws IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      return ImmutableMap.of();
    }
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      Map<String, String> args = GSON.fromJson(reader, STRING_MAP_TYPE);
      return args == null ? ImmutableMap.<String, String>of() : args;
    } catch (JsonSyntaxException e) {
      LOG.info("Failed to parse runtime arguments on {}", request.getUri(), e);
      throw e;
    } finally {
      reader.close();
    }
  }

  protected final void getAppRecords(HttpResponder responder, Store store, String namespaceId, String appId) {
    if (appId != null && appId.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "app-id is empty");
      return;
    }

    try {
      Id.Namespace accId = Id.Namespace.from(namespaceId);
      List<ApplicationRecord> appRecords = Lists.newArrayList();
      List<ApplicationSpecification> specList;
      if (appId == null) {
        specList = new ArrayList<ApplicationSpecification>(store.getAllApplications(accId));
      } else {
        ApplicationSpecification appSpec = store.getApplication(new Id.Application(accId, appId));
        if (appSpec == null) {
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
          return;
        }
        specList = Collections.singletonList(store.getApplication(new Id.Application(accId, appId)));
      }

      for (ApplicationSpecification appSpec : specList) {
        appRecords.add(new ApplicationRecord(appSpec.getName(), appSpec.getDescription()));
      }

      if (appId == null) {
        responder.sendJson(HttpResponseStatus.OK, appRecords);
      } else {
        responder.sendJson(HttpResponseStatus.OK, appRecords.get(0));
      }
    } catch (SecurityException e) {
      LOG.debug("Security Exception while retrieving app details: ", e);
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception : ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  protected final void programList(HttpResponder responder, String namespaceId, ProgramType type,
                                   @Nullable String applicationId, Store store) {
    if (applicationId != null && applicationId.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Application id is empty");
      return;
    }

    try {
      List<ProgramRecord> programRecords;
      if (applicationId == null) {
        Id.Namespace accId = Id.Namespace.from(namespaceId);
        programRecords = listPrograms(accId, type, store);
      } else {
        Id.Application appId = Id.Application.from(namespaceId, applicationId);
        programRecords = listProgramsByApp(appId, type, store);
      }

      if (programRecords == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        responder.sendJson(HttpResponseStatus.OK, programRecords);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  protected final List<ProgramRecord> listPrograms(Id.Namespace namespaceId, ProgramType type, Store store)
    throws Exception {
    try {
      Collection<ApplicationSpecification> appSpecs = store.getAllApplications(namespaceId);
      return listPrograms(appSpecs, type);
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      String errorMessage = String.format("Could not retrieve application spec for namespace '%s', reason: %s",
                                           namespaceId.toString(), throwable.getMessage());
      throw new Exception(errorMessage, throwable);
    }
  }

  /**
   * Return a list of {@link ProgramRecord} for a {@link ProgramType} in an Application. The return value may be
   * null if the applicationId does not exist.
   */
  private List<ProgramRecord> listProgramsByApp(Id.Application appId, ProgramType type, Store store) throws Exception {
    ApplicationSpecification appSpec;
    try {
      appSpec = store.getApplication(appId);
      return appSpec == null ? null : listPrograms(Collections.singletonList(appSpec), type);
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      String errorMessage = String.format("Could not retrieve application spec for application id '%s', reason: %s",
                                          appId.toString(), throwable.getMessage());
      throw new Exception(errorMessage, throwable);
    }
  }

  @Nullable
  protected ProgramType getProgramType(String programType) {
    try {
      return ProgramType.valueOfCategoryName(programType);
    } catch (Exception e) {
      return null;
    }
  }

  protected final List<ProgramRecord> listPrograms(Collection<ApplicationSpecification> appSpecs,
                                                   ProgramType type) throws Exception {
    List<ProgramRecord> programRecords = Lists.newArrayList();
    for (ApplicationSpecification appSpec : appSpecs) {
      switch (type) {
        case FLOW:
          createProgramRecords(appSpec.getName(), type, appSpec.getFlows().values(), programRecords);
          break;
        case PROCEDURE:
          createProgramRecords(appSpec.getName(), type, appSpec.getProcedures().values(), programRecords);
          break;
        case MAPREDUCE:
          createProgramRecords(appSpec.getName(), type, appSpec.getMapReduce().values(), programRecords);
          break;
        case SPARK:
          createProgramRecords(appSpec.getName(), type, appSpec.getSpark().values(), programRecords);
          break;
        case SERVICE:
          createProgramRecords(appSpec.getName(), type, appSpec.getServices().values(), programRecords);
          break;
        case WORKER:
          createProgramRecords(appSpec.getName(), type, appSpec.getWorkers().values(), programRecords);
          break;
        case WORKFLOW:
          createProgramRecords(appSpec.getName(), type, appSpec.getWorkflows().values(), programRecords);
          break;
        default:
          throw new Exception("Unknown program type: " + type.name());
      }
    }
    return programRecords;
  }

  private void createProgramRecords(String appId, ProgramType type,
                                    Iterable<? extends ProgramSpecification> programSpecs,
                                    List<ProgramRecord> programRecords) {
    for (ProgramSpecification programSpec : programSpecs) {
      programRecords.add(makeProgramRecord(appId, programSpec, type));
    }
  }

  protected static ProgramRecord makeProgramRecord(String appId, ProgramSpecification spec, ProgramType type) {
    return new ProgramRecord(type, appId, spec.getName(), spec.getDescription());
  }

  protected ProgramRuntimeService.RuntimeInfo findRuntimeInfo(String namespaceId, String appId,
                                                              String flowId, ProgramType typeId,
                                                              ProgramRuntimeService runtimeService) {
    ProgramType type = ProgramType.valueOf(typeId.name());
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(type).values();
    Preconditions.checkNotNull(runtimeInfos, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND),
                               namespaceId, flowId);

    Id.Program programId = Id.Program.from(namespaceId, appId, type, flowId);

    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (programId.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
  }

  protected void getLiveInfo(HttpResponder responder, String namespaceId,
                             final String appId, final String programId, ProgramType type,
                             ProgramRuntimeService runtimeService) {
    try {
      responder.sendJson(HttpResponseStatus.OK,
                         runtimeService.getLiveInfo(Id.Program.from(namespaceId, appId, type, programId), type));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Respond with a 404 if a NoSuchElementException is thrown.
   */
  protected boolean respondIfElementNotFound(Throwable t, HttpResponder responder) {
    return respondIfRootCauseOf(t, NoSuchElementException.class, HttpResponseStatus.NOT_FOUND, responder,
                                "Could not find element.");
  }

  private <T extends Throwable> boolean respondIfRootCauseOf(Throwable t, Class<T> type, HttpResponseStatus status,
                                                             HttpResponder responder, String msgFormat,
                                                             Object... args) {
    if (type.isAssignableFrom(Throwables.getRootCause(t).getClass())) {
      responder.sendString(status, String.format(msgFormat, args));
      return true;
    }
    return false;
  }

  // TODO: refactor
  protected final void dataList(HttpResponder responder, Store store, DatasetFramework dsFramework,
                                Data type, String namespaceId, String name, String appId) {
    try {
      if ((name != null && name.isEmpty()) || (appId != null && appId.isEmpty())) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Empty name provided");
        return;
      }

      Id.Namespace namespace = Id.Namespace.from(namespaceId);

      String json;
      if (name != null) {
        json = getDataEntity(store, dsFramework, namespace, type, name);
      } else if (appId != null) {
        Id.Application app = Id.Application.from(namespace, appId);
        json = listDataEntitiesByApp(store, dsFramework, app, type);
      } else {
        json = listDataEntities(store, dsFramework, namespace, type);
      }

      if (json.isEmpty()) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        responder.sendByteArray(HttpResponseStatus.OK, json.getBytes(Charsets.UTF_8),
                                ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception : ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private String getDataEntity(Store store, DatasetFramework dsFramework,
                               Id.Namespace namespace, Data type, String name) {
    if (type == Data.DATASET) {
      DatasetSpecification dsSpec = getDatasetSpec(dsFramework, namespace, name);
      return dsSpec == null ? "" : GSON.toJson(new DatasetRecord(name, dsSpec.getType()));
    } else if (type == Data.STREAM) {
      StreamSpecification spec = store.getStream(namespace, name);
      return spec == null ? "" : GSON.toJson(makeStreamRecord(spec.getName(), spec));
    }
    return "";
  }

  private String listDataEntities(Store store, DatasetFramework dsFramework,
                                  Id.Namespace namespace, Data type) throws Exception {
    if (type == Data.DATASET) {
      Collection<DatasetSpecificationSummary> instances = dsFramework.getInstances(namespace);
      List<DatasetRecord> result = Lists.newArrayListWithExpectedSize(instances.size());
      for (DatasetSpecificationSummary instance : instances) {
        result.add(new DatasetRecord(instance.getName(), instance.getType()));
      }
      return GSON.toJson(result);
    } else if (type == Data.STREAM) {
      Collection<StreamSpecification> specs = store.getAllStreams(namespace);
      List<StreamRecord> result = Lists.newArrayListWithExpectedSize(specs.size());
      for (StreamSpecification spec : specs) {
        result.add(makeStreamRecord(spec.getName(), null));
      }
      return GSON.toJson(result);
    }
    return "";

  }

  private String listDataEntitiesByApp(Store store, DatasetFramework dsFramework,
                                       Id.Application app, Data type) throws Exception {
    ApplicationSpecification appSpec = store.getApplication(app);
    if (appSpec == null) {
      return "";
    }
    if (type == Data.DATASET) {
      Set<String> dataSetsUsed = dataSetsUsedBy(appSpec);
      List<DatasetRecord> result = Lists.newArrayListWithExpectedSize(dataSetsUsed.size());
      for (String dsName : dataSetsUsed) {
        String typeName = null;
        DatasetSpecification dsSpec = getDatasetSpec(dsFramework, app.getNamespace(), dsName);
        if (dsSpec != null) {
          typeName = dsSpec.getType();
        }
        result.add(new DatasetRecord(dsName, typeName));
      }
      return GSON.toJson(result);
    }
    if (type == Data.STREAM) {
      Set<String> streamsUsed = streamsUsedBy(appSpec);
      List<StreamRecord> result = Lists.newArrayListWithExpectedSize(streamsUsed.size());
      for (String streamName : streamsUsed) {
        result.add(makeStreamRecord(streamName, null));
      }
      return GSON.toJson(result);
    }
    return "";
  }

  @Nullable
  private DatasetSpecification getDatasetSpec(DatasetFramework dsFramework, Id.Namespace namespaceId, String dsName) {
    try {
      return dsFramework.getDatasetSpec(Id.DatasetInstance.from(namespaceId, dsName));
    } catch (Exception e) {
      LOG.warn("Couldn't get spec for dataset: " + dsName);
      return null;
    }
  }

  private Set<String> dataSetsUsedBy(FlowSpecification flowSpec) {
    Set<String> result = Sets.newHashSet();
    for (FlowletDefinition flowlet : flowSpec.getFlowlets().values()) {
      result.addAll(flowlet.getDatasets());
    }
    return result;
  }

  private Set<String> dataSetsUsedBy(ApplicationSpecification appSpec) {
    Set<String> result = Sets.newHashSet();
    for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
      result.addAll(dataSetsUsedBy(flowSpec));
    }
    for (ProcedureSpecification procSpec : appSpec.getProcedures().values()) {
      result.addAll(procSpec.getDataSets());
    }
    for (MapReduceSpecification mrSpec : appSpec.getMapReduce().values()) {
      result.addAll(mrSpec.getDataSets());
    }
    return result;
  }

  private Set<String> streamsUsedBy(FlowSpecification flowSpec) {
    Set<String> result = Sets.newHashSet();
    for (FlowletConnection con : flowSpec.getConnections()) {
      if (FlowletConnection.Type.STREAM == con.getSourceType()) {
        result.add(con.getSourceName());
      }
    }
    return result;
  }

  private Set<String> streamsUsedBy(ApplicationSpecification appSpec) {
    Set<String> result = Sets.newHashSet();
    for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
      result.addAll(streamsUsedBy(flowSpec));
    }
    result.addAll(appSpec.getStreams().keySet());
    return result;
  }

  protected final void programListByDataAccess(HttpResponder responder,
                                               Store store, DatasetFramework dsFramework,
                                               ProgramType type, Data data, String namespaceId, String name) {
    try {
      if (name.isEmpty()) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, data.prettyName().toLowerCase() + " name is empty");
        return;
      }
      Id.Namespace namespace = Id.Namespace.from(namespaceId);
      List<ProgramRecord> programRecords = listProgramsByDataAccess(store, dsFramework, namespace, type, data, name);
      if (programRecords == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        responder.sendJson(HttpResponseStatus.OK, programRecords);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * @return list of program records, an empty list if no programs were found, or null if the stream or
   * dataset does not exist
   */
  private List<ProgramRecord> listProgramsByDataAccess(Store store, DatasetFramework dsFramework,
                                                       Id.Namespace namespace, ProgramType type,
                                                       Data data, String name) throws Exception {
    // search all apps for programs that use this
    List<ProgramRecord> result = Lists.newArrayList();
    Collection<ApplicationSpecification> appSpecs = store.getAllApplications(namespace);
    if (appSpecs != null) {
      for (ApplicationSpecification appSpec : appSpecs) {
        if (type == ProgramType.FLOW) {
          for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
            if ((data == Data.DATASET && usesDataSet(flowSpec, name))
              || (data == Data.STREAM && usesStream(flowSpec, name))) {
              result.add(makeProgramRecord(appSpec.getName(), flowSpec, ProgramType.FLOW));
            }
          }
        } else if (type == ProgramType.PROCEDURE) {
          for (ProcedureSpecification procedureSpec : appSpec.getProcedures().values()) {
            if (data == Data.DATASET && procedureSpec.getDataSets().contains(name)) {
              result.add(makeProgramRecord(appSpec.getName(), procedureSpec, ProgramType.PROCEDURE));
            }
          }
        } else if (type == ProgramType.MAPREDUCE) {
          for (MapReduceSpecification mrSpec : appSpec.getMapReduce().values()) {
            if (data == Data.DATASET && mrSpec.getDataSets().contains(name)) {
              result.add(makeProgramRecord(appSpec.getName(), mrSpec, ProgramType.MAPREDUCE));
            }
          }
        } else if (type == ProgramType.WORKER) {
          for (WorkerSpecification workerSpec : appSpec.getWorkers().values()) {
            if (data == Data.DATASET && workerSpec.getDatasets().contains(name)) {
              result.add(makeProgramRecord(appSpec.getName(), workerSpec, ProgramType.WORKER));
            }
          }
        }
      }
    }
    if (!result.isEmpty()) {
      return result;
    }
    // if no programs were found, check whether the data exists, return [] if yes, null if not
    boolean exists = false;
    if (data == Data.DATASET) {
      exists = dsFramework.hasInstance(Id.DatasetInstance.from(namespace, name));
    } else if (data == Data.STREAM) {
      exists = store.getStream(new Id.Namespace(Constants.DEFAULT_NAMESPACE), name) != null;
    }
    return exists ? result : null;
  }

  private static boolean usesDataSet(FlowSpecification flowSpec, String dataset) {
    for (FlowletDefinition flowlet : flowSpec.getFlowlets().values()) {
      if (flowlet.getDatasets().contains(dataset)) {
        return true;
      }
    }
    return false;
  }

  private static boolean usesStream(FlowSpecification flowSpec, String stream) {
    for (FlowletConnection con : flowSpec.getConnections()) {
      if (FlowletConnection.Type.STREAM == con.getSourceType() && stream.equals(con.getSourceName())) {
        return true;
      }
    }
    return false;
  }

  /* -----------------  helpers to return Json consistently -------------- */

  protected static StreamRecord makeStreamRecord(String name, StreamSpecification specification) {
    return new StreamRecord(name, GSON.toJson(specification));
  }
}
