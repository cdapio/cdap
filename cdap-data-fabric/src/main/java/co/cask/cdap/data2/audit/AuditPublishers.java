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

package co.cask.cdap.data2.audit;

import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.access.AccessPayload;
import co.cask.cdap.proto.id.EntityIdCompatible;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * Helper class to publish audit.
 */
public final class AuditPublishers {
  private static final Logger LOG = LoggerFactory.getLogger(AuditPublishers.class);
  private static final AtomicBoolean WARNING_LOGGED = new AtomicBoolean(false);
  private static final Integer ACCESS_CACHE_MAX_SIZE = 1024;
  private static final Cache<AccessAuditInfo, Boolean> CACHE_AUDIT_LOGS =
    CacheBuilder.newBuilder().expireAfterWrite(1L, TimeUnit.DAYS).maximumSize(ACCESS_CACHE_MAX_SIZE).build();

  private AuditPublishers() {}

  /**
   * Publish access audit information using {@link AuditPublisher}.
   *
   * @param publisher audit publisher, if null no audit information is published
   * @param entityId entity id for which audit information is being published
   * @param accessType access type
   * @param accessor the entity accessing entityId
   */
  public static void publishAccess(@Nullable AuditPublisher publisher, EntityIdCompatible entityId,
                                   AccessType accessType, EntityIdCompatible accessor) {
    if (publisher == null) {
      logWarning();
      return;
    }

    AccessAuditInfo accessAuditInfo = new AccessAuditInfo(accessor, entityId, accessType);
    synchronized (CACHE_AUDIT_LOGS) {
      if (CACHE_AUDIT_LOGS.getIfPresent(accessAuditInfo) != null) {
        // this access has already been published recently (since it is present in the cache). hence don't publish again
        return;
      }
      CACHE_AUDIT_LOGS.put(accessAuditInfo, true);
    }

    switch (accessType) {
      case READ:
        publisher.publish(entityId.toEntityId(), AuditType.ACCESS,
                          new AccessPayload(co.cask.cdap.proto.audit.payload.access.AccessType.READ,
                                            accessor.toEntityId()));
        break;
      case WRITE:
        publisher.publish(entityId.toEntityId(), AuditType.ACCESS,
                          new AccessPayload(co.cask.cdap.proto.audit.payload.access.AccessType.WRITE,
                                            accessor.toEntityId()));
        break;
      case READ_WRITE:
        publisher.publish(entityId.toEntityId(), AuditType.ACCESS,
                          new AccessPayload(co.cask.cdap.proto.audit.payload.access.AccessType.READ,
                                            accessor.toEntityId()));
        publisher.publish(entityId.toEntityId(), AuditType.ACCESS,
                          new AccessPayload(co.cask.cdap.proto.audit.payload.access.AccessType.WRITE,
                                            accessor.toEntityId()));
        break;
      case UNKNOWN:
        publisher.publish(entityId.toEntityId(), AuditType.ACCESS,
                          new AccessPayload(co.cask.cdap.proto.audit.payload.access.AccessType.UNKNOWN,
                                            accessor.toEntityId()));
        break;
    }
  }

  /**
   * Publish audit information using {@link AuditPublisher}.
   *
   * @param publisher audit publisher, if null no audit information is published
   * @param entityId entity id for which audit information is being published
   * @param auditType audit type
   * @param auditPayload audit payload
   */
  public static void publishAudit(@Nullable AuditPublisher publisher, EntityIdCompatible entityId,
                                  AuditType auditType, AuditPayload auditPayload) {
    if (publisher == null) {
      logWarning();
      return;
    }

    publisher.publish(entityId.toEntityId(), auditType, auditPayload);
  }

  /**
   * Logs warning about not having audit publisher. The warning is logged only once.
   */
  private static void logWarning() {
    if (!WARNING_LOGGED.get()) {
      LOG.warn("Audit publisher is null, audit information will not be published");
      WARNING_LOGGED.set(true);
    }
  }

  /**
   * Contains the accessed entity info and the access type.
   */
  private static class AccessAuditInfo {
    private final EntityIdCompatible accessorEntity;
    private final EntityIdCompatible accessedEntity;
    private final AccessType accessType;

    AccessAuditInfo(EntityIdCompatible accessorEntity, EntityIdCompatible accessedEntity, AccessType accessType) {
      this.accessorEntity = accessorEntity;
      this.accessedEntity = accessedEntity;
      this.accessType = accessType;
    }

    public EntityIdCompatible getAccessorEntity() {
      return accessorEntity;
    }

    public EntityIdCompatible getAccessedEntity() {
      return accessedEntity;
    }

    public AccessType getAccessType() {
      return accessType;
    }

    @Override
    public int hashCode() {
      return Objects.hash(accessorEntity, accessedEntity, accessType);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }

      AccessAuditInfo that = (AccessAuditInfo) obj;
      return Objects.equals(accessorEntity, that.accessorEntity) &&
        Objects.equals(accessedEntity, that.accessedEntity) &&
        Objects.equals(accessType, that.accessType);
    }

    @Override
    public String toString() {
      return "AccessedEntityInfo{" +
        "accessorEntity='" + accessorEntity + '\'' +
        "accessedEntity='" + accessedEntity + '\'' +
        ", accessType='" + accessType +
        '}';
    }
  }
}
