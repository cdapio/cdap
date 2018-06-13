/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.proto.codec;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.access.AccessPayload;
import co.cask.cdap.proto.audit.payload.metadata.MetadataPayload;
import co.cask.cdap.proto.id.EntityId;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;

/**
 * Code for {@link AuditMessage}.
 */
public class AuditMessageTypeAdapter implements JsonDeserializer<AuditMessage> {
  @Override
  public AuditMessage deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    long timeMillis = jsonObj.get("time").getAsLong();
    MetadataEntity metadataEntity;
    EntityId entityId = context.deserialize(jsonObj.getAsJsonObject("entityId"), EntityId.class);
    if (entityId != null) {
      metadataEntity = entityId.toMetadataEntity();
    } else {
      metadataEntity = context.deserialize(jsonObj.getAsJsonObject("metadataEntity"), MetadataEntity.class);
    }
    String user = jsonObj.get("user").getAsString();
    AuditType auditType = context.deserialize(jsonObj.getAsJsonPrimitive("type"), AuditType.class);

    AuditPayload payload;
    JsonObject jsonPayload = jsonObj.getAsJsonObject("payload");
    switch (auditType) {
      case METADATA_CHANGE:
        payload = context.deserialize(jsonPayload, MetadataPayload.class);
        break;
      case ACCESS:
        payload = context.deserialize(jsonPayload, AccessPayload.class);
        break;
      default:
        payload = AuditPayload.EMPTY_PAYLOAD;
    }
    return new AuditMessage(timeMillis, metadataEntity, user, auditType, payload);
  }
}
