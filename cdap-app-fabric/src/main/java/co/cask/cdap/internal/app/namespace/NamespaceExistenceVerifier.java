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

package co.cask.cdap.internal.app.namespace;

import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.entity.EntityExistenceVerifier;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

/**
 * {@link EntityExistenceVerifier} for {@link NamespaceId namespaces}.
 */
public class NamespaceExistenceVerifier implements EntityExistenceVerifier<NamespaceId> {

  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  NamespaceExistenceVerifier(NamespaceQueryAdmin namespaceQueryAdmin) {
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }


  @Override
  public void ensureExists(NamespaceId namespaceId) throws NotFoundException {
    if (!NamespaceId.SYSTEM.equals(namespaceId)) {
      boolean exists = false;
      try {
        exists = namespaceQueryAdmin.exists(namespaceId.toId());
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
      if (!exists) {
        throw new NamespaceNotFoundException(namespaceId.toId());
      }
    }
  }
}
