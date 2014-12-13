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

package co.cask.cdap.gateway.handlers.util;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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

  protected final void programList(HttpRequest request, HttpResponder responder,
                                   ProgramType type, @Nullable String applicationId, Store store) {
    if (applicationId != null && applicationId.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Application id is empty");
      return;
    }

    try {
      String accountId = getAuthenticatedAccountId(request);
      List<ProgramRecord> programRecords;
      if (applicationId == null) {
        Id.Account accId = Id.Account.from(accountId);
        programRecords = listPrograms(accId, type, store);
      } else {
        Id.Application appId = Id.Application.from(accountId, applicationId);
        programRecords = listProgramsByApp(appId, type, store);
      }

      if (programRecords == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        String result = GSON.toJson(programRecords);
        responder.sendByteArray(HttpResponseStatus.OK, result.getBytes(Charsets.UTF_8),
                                ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  protected final List<ProgramRecord> listPrograms(Id.Account accId, ProgramType type, Store store) throws Exception {
    try {
      Collection<ApplicationSpecification> appSpecs = store.getAllApplications(accId);
      return listPrograms(appSpecs, type);
    } catch (Throwable throwable) {
      LOG.warn(throwable.getMessage(), throwable);
      String errorMessage = String.format("Could not retrieve application spec for %s, reason: %s",
                                           accId.toString(), throwable.getMessage());
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
      String errorMessage = String.format("Could not retrieve application spec for %s, reason: %s",
                                          appId.toString(), throwable.getMessage());
      throw new Exception(errorMessage, throwable);
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
    return new ProgramRecord(type, appId, spec.getName(), spec.getName(), spec.getDescription());
  }

  protected ProgramRuntimeService.RuntimeInfo findRuntimeInfo(String accountId, String appId,
                                                              String flowId, ProgramType typeId,
                                                              ProgramRuntimeService runtimeService) {
    ProgramType type = ProgramType.valueOf(typeId.name());
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(type).values();
    Preconditions.checkNotNull(runtimeInfos, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND),
                               accountId, flowId);

    Id.Program programId = Id.Program.from(accountId, appId, flowId);

    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (programId.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
  }

  protected void getLiveInfo(HttpRequest request, HttpResponder responder,
                             final String appId, final String programId, ProgramType type,
                             ProgramRuntimeService runtimeService) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      responder.sendJson(HttpResponseStatus.OK,
                         runtimeService.getLiveInfo(Id.Program.from(accountId,
                                                                    appId,
                                                                    programId),
                                                    type));
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
                                "Could not find element.", null);
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
}
