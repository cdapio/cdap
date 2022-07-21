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

package io.cdap.cdap.gateway.handlers.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.internal.UserErrors;
import io.cdap.cdap.internal.UserMessages;
import io.cdap.cdap.proto.Instances;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.PermissionAdapterFactory;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.api.logging.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Abstract Class that contains commonly used methods for parsing Http Requests.
 */
public abstract class AbstractAppFabricHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAppFabricHttpHandler.class);

  /**
   * Json serializer.
   */
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .registerTypeAdapterFactory(new PermissionAdapterFactory())
    .create();

  protected static final Type MAP_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  protected static final Type SET_STRING_TYPE = new TypeToken<Set<String>>() { }.getType();

  /**
   * Name of the header that should specify the application archive
   */
  public static final String ARCHIVE_NAME_HEADER = "X-Archive-Name";

  public static final String APP_CONFIG_HEADER = "X-App-Config";

  public static final String PRINCIPAL_HEADER = "X-Principal";
  public static final String SCHEDULES_HEADER = "X-App-Deploy-Update-Schedules";

  protected int getInstances(FullHttpRequest request) throws BadRequestException {
    Instances instances;
      try {
        instances = parseBody(request, Instances.class);
      } catch (JsonSyntaxException e) {
        throw new BadRequestException("Invalid JSON in request: " + e.getMessage());
      }
    if (instances == null) {
      throw new BadRequestException("Invalid instance value in request");
    }
    return instances.getInstances();
  }

  @Nullable
  protected <T> T parseBody(FullHttpRequest request, Type type) throws IllegalArgumentException, JsonSyntaxException {
    ByteBuf content = request.content();
    if (!content.isReadable()) {
      return null;
    }
    Reader reader = new InputStreamReader(new ByteBufInputStream(content), StandardCharsets.UTF_8);
    try {
      return GSON.fromJson(reader, type);
    } catch (RuntimeException e) {
      LOG.info("Failed to parse body on {} as {}", request.uri(), type, e);
      throw e;
    } finally {
      Closeables.closeQuietly(reader);
    }
  }

  protected Map<String, String> decodeArguments(FullHttpRequest request) throws JsonSyntaxException {
    Map<String, String> args = parseBody(request, MAP_STRING_TYPE);
    if (args == null) {
      return ImmutableMap.of();
    }
    return args;
  }

  protected ProgramRuntimeService.RuntimeInfo findRuntimeInfo(ProgramId programId,
                                                              ProgramRuntimeService runtimeService) {
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(programId.getType()).values();
    Preconditions.checkNotNull(runtimeInfos, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND), programId);

    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (programId.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
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

  /**
   * Helper method to transform the type of the log level map.
   */
  protected Map<String, LogEntry.Level> transformLogLevelsMap(Map<String, String> logLevels) {
    return Maps.transformValues(logLevels, new Function<String, LogEntry.Level>() {
      @Override
      @Nullable
      public LogEntry.Level apply(@Nullable String input) {
        return input == null ? null : LogEntry.Level.valueOf(input.toUpperCase());
      }
    });
  }
}
