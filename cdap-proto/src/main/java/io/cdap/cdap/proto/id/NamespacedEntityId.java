/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.proto.id;

import co.cask.cdap.proto.element.EntityType;

/**
 * An {@link EntityId} which belongs to a namespace.
 */
public abstract class NamespacedEntityId extends EntityId {

  protected final String namespace;

  protected NamespacedEntityId(String namespace, EntityType entity) {
    super(entity);
    if (namespace == null) {
      throw new NullPointerException("Namespace can not be null.");
    }
    ensureValidNamespace(namespace);
    this.namespace = namespace;
  }

  public String getNamespace() {
    return namespace;
  }

  public NamespaceId getNamespaceId() {
    return new NamespaceId(getNamespace());
  }
}
