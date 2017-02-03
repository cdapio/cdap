/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.security.impersonation;

import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;

import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Delegates to impersonator's doAs, but uses the namespace passed during initialization.
 */
public class EntityImpersonator {
  private final NamespacedEntityId entityId;
  private final Impersonator impersonator;

  public EntityImpersonator(@Nullable NamespacedEntityId entityId, Impersonator impersonator) {
    this.entityId = entityId;
    this.impersonator = impersonator;
  }

  /**
   * use impersonator to call the passed callable,
   * use namespaceId of the instance while calling the doAs of impersonator, return the result of callable.
   * @param callable callable
   * @param <T> callable return type
   * @return result of callable
   */
  public <T> T impersonate(final Callable<T> callable) throws Exception {
    // todo entityId shouldn't be null, it's passed null only from PluginService. which needs to be updated.
    if (entityId == null || entityId.getNamespaceId().equals(NamespaceId.SYSTEM)) {
      // do not impersonate for system namespace
      return callable.call();
    }
    return impersonator.doAs(entityId, new Callable<T>() {
      @Override
      public T call() throws Exception {
        return callable.call();
      }
    });
  }
}
