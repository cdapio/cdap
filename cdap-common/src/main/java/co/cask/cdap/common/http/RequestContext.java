/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.common.http;

import co.cask.cdap.api.security.EntityId;

/**
 * Maintains properties relevant to the current request.
 */
public final class RequestContext {
  private static final ThreadLocal<EntityId> entityId = new InheritableThreadLocal<EntityId>();
  private static final ThreadLocal<String> userId = new InheritableThreadLocal<String>();

  public static void setEntityId(EntityId entityIdParam) {
    entityId.set(entityIdParam);
  }

  public static EntityId getEntityId() {
    return entityId.get();
  }

  public static String getUserId() {
    return userId.get();
  }

  public static void setUserId(String userIdParam) {
    userId.set(userIdParam);
  }
}
