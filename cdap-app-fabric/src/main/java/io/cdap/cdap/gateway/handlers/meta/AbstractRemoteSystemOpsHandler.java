/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers.meta;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.internal.remote.MethodArgument;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ConnectionEntityId;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.cdap.proto.id.WorkerId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import io.cdap.http.AbstractHttpHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implements common functionality for reading method arguments and deserialize them.
 */
class AbstractRemoteSystemOpsHandler extends AbstractHttpHandler {

  private static final Gson GSON = new Gson();
  private static final Type METHOD_ARGUMENT_LIST_TYPE = new TypeToken<List<MethodArgument>>() {
  }.getType();
  private static final Map<String, Class<?>> ALLOWED_ARGUMENT_TYPES;

  static {
    Map<String, Class<?>> types = new HashMap<>();
    types.put(Principal.class.getName(), Principal.class);
    types.put(Role.class.getName(), Role.class);
    for (EntityType entityType : EntityType.values()) {
      types.put(entityType.getIdClass().getName(), entityType.getIdClass());
    }
    // Concrete entity id classes that EntityType does not list.
    for (Class<?> type : Arrays.asList(ServiceId.class, WorkerId.class, WorkflowId.class,
        ConnectionEntityId.class)) {
      types.put(type.getName(), type);
    }
    ALLOWED_ARGUMENT_TYPES = Collections.unmodifiableMap(types);
  }

  // we don't share the same version as other handlers in app fabric, so we can upgrade/iterate faster
  protected static final String VERSION = "/v1";

  Iterator<MethodArgument> parseArguments(FullHttpRequest request) {
    String body = request.content().toString(StandardCharsets.UTF_8);
    List<MethodArgument> arguments = GSON.fromJson(body, METHOD_ARGUMENT_LIST_TYPE);
    return arguments.iterator();
  }

  @Nullable
  <T> T deserializeNext(Iterator<MethodArgument> arguments) throws BadRequestException {
    return deserializeNext(arguments, null);
  }

  @Nullable
  <T> T deserializeNext(Iterator<MethodArgument> arguments,
      @Nullable Type typeOfT) throws BadRequestException {
    if (!arguments.hasNext()) {
      throw new BadRequestException("Expected additional elements.");
    }

    MethodArgument argument = arguments.next();
    if (argument == null) {
      return null;
    }
    JsonElement value = argument.getValue();
    if (value == null) {
      return null;
    }
    if (typeOfT != null) {
      return GSON.fromJson(value, typeOfT);
    }
    Class<?> type = ALLOWED_ARGUMENT_TYPES.get(argument.getType());
    if (type == null) {
      throw new BadRequestException("Unsupported argument type " + argument.getType());
    }
    return GSON.<T>fromJson(value, type);
  }
}
