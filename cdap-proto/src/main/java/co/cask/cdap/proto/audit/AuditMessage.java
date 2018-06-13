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

package co.cask.cdap.proto.audit;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.proto.id.EntityId;

import java.util.Objects;

/**
 * Represents a change on an entity that needs to be audited.
 */
@Beta
public class AuditMessage {
  // version 1 has consisted of EntityId and version 2 consists of MetadataEntity
  private final int version = 2;

  private final long time;
  private final MetadataEntity metadataEntity;
  private final String user;
  private final AuditType type;
  private final AuditPayload payload;

  public AuditMessage(long time, EntityId entityId, String user, AuditType type, AuditPayload payload) {
    this(time, entityId.toMetadataEntity(), user, type, payload);
  }

  public AuditMessage(long time, MetadataEntity metadataEntity, String user, AuditType type, AuditPayload payload) {
    this.time = time;
    this.metadataEntity = metadataEntity;
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

  public MetadataEntity getEntity() {
    return metadataEntity;
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
      Objects.equals(metadataEntity, that.metadataEntity) &&
      Objects.equals(user, that.user) &&
      Objects.equals(type, that.type) &&
      Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, time, metadataEntity, user, type, payload);
  }

  @Override
  public String toString() {
    return "AuditMessage{" +
      "version=" + version +
      ", time=" + time +
      ", metadataEntity=" + metadataEntity +
      ", user='" + user + '\'' +
      ", type=" + type +
      ", payload=" + payload +
      '}';
  }
}
