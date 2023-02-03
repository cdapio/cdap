/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.cdap.proto.artifact;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import io.cdap.cdap.proto.id.EntityId;

/**
 * A container for messages in the app deletion topic.
 * It carries the payload as {@link JsonElement} which consists of the app-id.
 */
public final class AppDeletionMessage {
    private final EntityId entityId;
    private final JsonElement payload;

    /**
     * Creates an instance for an entity.
     *
     * @param entityId the {@link EntityId} of the entity emitting this message
     * @param payload the payload
     */
    public AppDeletionMessage(EntityId entityId, JsonElement payload) {
      this.entityId = entityId;
      this.payload = payload;
    }

    /**
     * Returns the {@link EntityId} of the entity who emit this message.
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

    /**
     * Returns the payload as the raw {@link JsonElement}.
     */
    public JsonElement getRawPayload() {
      return payload;
    }

    @Override
    public String toString() {
      return "AppDeletionMessage{" +
        "entityId=" + entityId +
        ", payload=" + payload +
        '}';
    }
}

