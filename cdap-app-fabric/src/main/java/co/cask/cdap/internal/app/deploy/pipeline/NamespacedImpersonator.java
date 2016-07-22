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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.data2.security.Impersonator;
import co.cask.cdap.proto.id.NamespaceId;

import java.util.concurrent.Callable;

/**
 * Delegates to impersonator's doAs, but uses the namespace passed during initialization.
 */
public class NamespacedImpersonator {
  private final NamespaceId namespaceId;
  private final Impersonator impersonator;

  public NamespacedImpersonator(NamespaceId namespaceId, Impersonator impersonator) {
    this.namespaceId = namespaceId;
    this.impersonator = impersonator;
  }

  /**
   * use impersonator to call the passed callable,
   * use namespaceId of the instance while calling the doAs of impersonator, return the result of callable.
   * @param callable callable
   * @param <T> callable return type
   * @return result of callable
   * @throws Exception
   */
  public  <T> T impersonate(final Callable<T> callable) throws Exception {
    // todo namespaceId shouldn't be null, it's passed null only from PluginService. which needs to be updated.
    if (namespaceId == null || namespaceId.equals(NamespaceId.SYSTEM)) {
      // do not impersonate for system namespace
      return callable.call();
    }
    return impersonator.doAs(namespaceId, new Callable<T>() {
      @Override
      public T call() throws Exception {
        return callable.call();
      }
    });
  }
}
