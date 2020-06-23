/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.app.preview;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.ProgramRunId;

/**
 * A container for messages in the preview topic configured by {@link Constants.Preview#MESSAGING_TOPIC}.
 * It carries the message type and the payload as {@link JsonElement}.
 */
public final class PreviewMessage {

  /**
   * The message type
   */
  public enum Type {
    DATA,
    STATUS,
    PROGRAM_RUN_ID
  }

  private final Type type;
  private final EntityId entityId;
  private final JsonElement payload;

  /**
   * Create an instance of message.
   * @param type type of the message
   * @param entityId program run id associated with the message
   * @param payload the payload
   */
  public PreviewMessage(Type type, EntityId entityId, JsonElement payload) {
    this.type = type;
    this.entityId = entityId;
    this.payload = payload;
  }

  /**
   * Returns the type of the message.
   */
  public Type getType() {
    return type;
  }

  /**
   * Returns the {@link ProgramRunId} of the associated with the preview.
   */
  public EntityId getEntityId() {
    return entityId;
  }

  /**
   * Returns the payload by decoding the json to the given type.
   *
   * @param gson the {@link Gson} for decoding the json element
   * @param objType the resulting object type
   * @param <T> the resulting object type
   * @return the decode object
   */
  public <T> T getPayload(Gson gson, java.lang.reflect.Type objType) {
    return gson.fromJson(payload, objType);
  }

  @Override
  public String toString() {
    return "PreviewMessage{" +
      "type=" + type +
      ", entityId=" + entityId +
      ", payload=" + payload +
      '}';
  }
}
