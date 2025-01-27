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

package io.cdap.cdap.gateway.handlers.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.ConstraintCodec;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.SatisfiableTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TriggerCodec;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.BatchProgram;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class ProgramHandlerUtil {

  private ProgramHandlerUtil() {
  }

  /**
   * Json serializer/deserializer.
   */
  private static final Gson GSON = ApplicationSpecificationAdapter
      .addTypeAdapters(new GsonBuilder())
      .registerTypeAdapter(Trigger.class, new TriggerCodec())
      .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
      .registerTypeAdapter(Constraint.class, new ConstraintCodec())
      .create();

  /**
   * Json serde for decoding request. It uses a case insensitive enum adapter.
   */
  private static final Gson DECODE_GSON = ApplicationSpecificationAdapter
      .addTypeAdapters(new GsonBuilder())
      .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
      .registerTypeAdapter(Trigger.class, new TriggerCodec())
      .registerTypeAdapter(SatisfiableTrigger.class, new TriggerCodec())
      .registerTypeAdapter(Constraint.class, new ConstraintCodec())
      .create();

  public static String toJson(Object object) {
    return GSON.toJson(object);
  }

  public static String toJson(Object object, @NotNull Type type) {
    return GSON.toJson(object, type);
  }

  public static <T> T fromJson(@NotNull Reader reader, Class<T> type) {
    return DECODE_GSON.fromJson(reader, type);
  }

  public static <T> T fromJson(@Nullable JsonElement json, Class<T> type) {
    return DECODE_GSON.fromJson(json, type);
  }

  public static <T extends BatchProgram> List<T> validateAndGetBatchInput(FullHttpRequest request,
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
}
