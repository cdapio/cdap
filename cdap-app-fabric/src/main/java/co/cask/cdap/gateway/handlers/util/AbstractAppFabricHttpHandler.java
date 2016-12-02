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

package co.cask.cdap.gateway.handlers.util;

import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.proto.Instances;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
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
    .create();

  protected static final java.lang.reflect.Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  /**
   * Name of the header that should specify the application archive
   */
  public static final String ARCHIVE_NAME_HEADER = "X-Archive-Name";

  public static final String APP_CONFIG_HEADER = "X-App-Config";

  protected int getInstances(HttpRequest request) throws BadRequestException {
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
  protected <T> T parseBody(HttpRequest request, Type type) throws IllegalArgumentException, JsonSyntaxException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      return null;
    }
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      return GSON.fromJson(reader, type);
    } catch (RuntimeException e) {
      LOG.info("Failed to parse body on {} as {}", request.getUri(), type, e);
      throw e;
    } finally {
      Closeables.closeQuietly(reader);
    }
  }

  protected Map<String, String> decodeArguments(HttpRequest request) throws JsonSyntaxException {
    Map<String, String> args = parseBody(request, STRING_MAP_TYPE);
    if (args == null) {
      return ImmutableMap.of();
    }
    return args;
  }

  @Nullable
  protected ProgramType getProgramType(String programType) {
    try {
      return ProgramType.valueOfCategoryName(programType);
    } catch (Exception e) {
      return null;
    }
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
}
