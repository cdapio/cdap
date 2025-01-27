/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.ConstraintCodec;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerCodec;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.BatchProgram;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.List;

public abstract class AbstractProgramLifecycleHttpHandler extends AbstractAppFabricHttpHandler {

  /**
   * Json serializer/deserializer.
   */
  protected static final Gson GSON = ApplicationSpecificationAdapter
      .addTypeAdapters(new GsonBuilder())
      .registerTypeAdapter(Trigger.class, new TriggerCodec())
      .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
      .registerTypeAdapter(Constraint.class, new ConstraintCodec())
      .create();

  /**
   * Json serde for decoding request. It uses a case insensitive enum adapter.
   */
  protected static final Gson DECODE_GSON = ApplicationSpecificationAdapter
      .addTypeAdapters(new GsonBuilder())
      .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
      .registerTypeAdapter(Trigger.class, new TriggerCodec())
      .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
      .registerTypeAdapter(Constraint.class, new ConstraintCodec())
      .create();

  protected final ProgramLifecycleService lifecycleService;
  protected final Store store;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  public AbstractProgramLifecycleHttpHandler(ProgramLifecycleService lifecycleService, Store store,
      NamespaceQueryAdmin namespaceQueryAdmin) {
    this.lifecycleService = lifecycleService;
    this.store = store;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  protected <T extends BatchProgram> List<T> validateAndGetBatchInput(FullHttpRequest request,
      Type type)
      throws BadRequestException, IOException {

    List<T> programs;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()),
        StandardCharsets.UTF_8)) {
      try {
        programs = DECODE_GSON.fromJson(reader, type);
        if (programs == null) {
          throw new BadRequestException(
              "Request body is invalid json, please check that it is a json array.");
        }
      } catch (JsonSyntaxException e) {
        throw new BadRequestException("Request body is invalid json: " + e.getMessage());
      }
    }

    // validate input
    for (BatchProgram program : programs) {
      try {
        program.validate();
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(
            "Must provide valid appId, programType, and programId for each object: "
                + e.getMessage());
      }
    }
    return programs;
  }

  /**
   * @param namespaceId namespace Id
   * @param appId app Id
   * @return latest app version
   */
  protected String getLatestAppVersion(NamespaceId namespaceId, String appId)
      throws ApplicationNotFoundException {
    ApplicationMeta latestApplicationMeta = store.getLatest(namespaceId.appReference(appId));
    if (latestApplicationMeta == null) {
      throw new ApplicationNotFoundException(
          new ApplicationReference(namespaceId.getNamespace(), appId));
    }
    return latestApplicationMeta.getSpec().getAppVersion();
  }

  protected NamespaceId validateAndGetNamespace(String namespace) throws NamespaceNotFoundException {
    NamespaceId namespaceId = new NamespaceId(namespace);
    try {
      namespaceQueryAdmin.get(namespaceId);
    } catch (NamespaceNotFoundException e) {
      throw e;
    } catch (Exception e) {
      // This can only happen when NamespaceAdmin uses HTTP to interact with namespaces.
      // Within AppFabric, NamespaceAdmin is bound to DefaultNamespaceAdmin which directly interacts with MDS.
      // Hence, this should never happen.
      throw Throwables.propagate(e);
    }
    return namespaceId;
  }

  /**
   * Parses the give program type into {@link ProgramType} object.
   *
   * @param programType the program type to parse.
   * @throws BadRequestException if the given program type is not a valid {@link ProgramType}.
   */
  protected ProgramType getProgramType(String programType) throws BadRequestException {
    try {
      return ProgramType.valueOfCategoryName(programType);
    } catch (Exception e) {
      throw new BadRequestException(String.format("Invalid program type '%s'", programType), e);
    }
  }

  /**
   * Fetches the run record for particular run of a program without version.
   *
   * @param namespace namespace id
   * @param appName application name
   * @param type program type
   * @param programName program name
   * @param runId the run id
   * @return run record for the specified program and runRef, null if not found
   */
  protected RunRecordDetail getRunRecordDetailFromId(String namespace, String appName,
      String type, String programName, String runId)
      throws BadRequestException, NotFoundException {
    ProgramType programType = getProgramType(type);
    ProgramReference programRef = new ApplicationReference(namespace, appName).program(programType,
        programName);
    RunRecordDetail runRecordMeta = store.getRun(programRef, runId);
    if (runRecordMeta == null) {
      throw new NotFoundException(
          String.format("No run record found for program %s and runID: %s", programRef, runId));
    }
    return runRecordMeta;
  }
}
