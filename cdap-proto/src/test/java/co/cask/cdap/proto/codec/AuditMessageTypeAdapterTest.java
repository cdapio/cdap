/*
 * Copyright © 2018 Cask Data, Inc.
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
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;

public class AuditMessageTypeAdapterTest {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(co.cask.cdap.proto.audit.AuditMessage.class, new AuditMessageTypeAdapter())
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  @Test
  public void testCreateMessage() throws Exception {
    // tests that the older AuditMessage which consits of EntityId can be deserialized back to the newer version
    DatasetId datasetId = new NamespaceId("ns1").dataset("ds1");
    MetadataEntity metadataEntity = datasetId.toMetadataEntity();
    AuditMessage v1AuditMessage = new AuditMessage(1000L, datasetId, "user1",
                                                   AuditType.CREATE, AuditPayload.EMPTY_PAYLOAD);
    co.cask.cdap.proto.audit.AuditMessage expectedV2AuditMessage =
      new co.cask.cdap.proto.audit.AuditMessage(1000L, metadataEntity, "user1",
                                                AuditType.CREATE, AuditPayload.EMPTY_PAYLOAD);
    co.cask.cdap.proto.audit.AuditMessage actualV2AuditMessage =
      GSON.fromJson(GSON.toJson(v1AuditMessage), co.cask.cdap.proto.audit.AuditMessage.class);
    Assert.assertEquals(expectedV2AuditMessage, actualV2AuditMessage);
  }
}

// Older AuditMessage class which used to have EntityId.
class AuditMessage {
  private final int version = 1;

  private final long time;
  private final EntityId entityId;
  private final String user;
  private final AuditType type;
  private final AuditPayload payload;

  AuditMessage(long time, EntityId entityId, String user, AuditType type, AuditPayload payload) {
    this.time = time;
    this.entityId = entityId;
    this.user = user;
    this.type = type;
    this.payload = payload;
  }

  public int getVersion() {
    return version;
  }

  public long getTime() {
    return time;
  }

  public EntityId getEntityId() {
    return entityId;
  }

  public String getUser() {
    return user;
  }

  public AuditType getType() {
    return type;
  }

  public AuditPayload getPayload() {
    return payload;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AuditMessage)) {
      return false;
    }
    AuditMessage that = (AuditMessage) o;
    return Objects.equals(version, that.version) &&
      Objects.equals(time, that.time) &&
      Objects.equals(entityId, that.entityId) &&
      Objects.equals(user, that.user) &&
      Objects.equals(type, that.type) &&
      Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, time, entityId, user, type, payload);
  }

  @Override
  public String toString() {
    return "AuditMessage{" +
      "version=" + version +
      ", time=" + time +
      ", entityId=" + entityId +
      ", user='" + user + '\'' +
      ", type=" + type +
      ", payload=" + payload +
      '}';
  }
}
