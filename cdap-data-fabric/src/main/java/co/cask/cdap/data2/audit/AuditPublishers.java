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
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.access.AccessPayload;
import co.cask.cdap.proto.id.EntityIdCompatible;

/**
 * Helper class to publish audit.
 */
public final class AuditPublishers {
  private AuditPublishers() {}

  /**
   * Publish access audit information.
   *
   * @param publisher instance of {@link AuditPublisher}
   * @param entityId entity id for which audit information is being published
   * @param accessType access type
   * @param accessor the entity accessing entityId
   */
  public static void publishAccess(AuditPublisher publisher, EntityIdCompatible entityId, AccessType accessType,
                                   EntityIdCompatible accessor) {
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
}
