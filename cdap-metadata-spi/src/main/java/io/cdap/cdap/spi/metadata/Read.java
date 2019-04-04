/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.spi.metadata;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;

import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A read from the metadata store.
 */
@Beta
public class Read {
  private final MetadataEntity entity;
  private final Set<MetadataScope> scopes;
  private final Set<MetadataKind> kinds;
  private final Set<ScopedNameOfKind> selection;

  /**
   * Read all metadata for an entity.
   */
  public Read(MetadataEntity entity) {
    this(entity, null, null, null);
  }

  /**
   * Read all metadata in a given scope for an entity.
   */
  public Read(MetadataEntity entity, MetadataScope scope) {
    this(entity, scope, null, null);
  }

  /**
   * Read all metadata of a given kind for an entity.
   */
  public Read(MetadataEntity entity, MetadataKind kind) {
    this(entity, null, kind, null);
  }

  /**
   * Read all metadata of a given kind in a given scope for an entity.
   */
  public Read(MetadataEntity entity, MetadataScope scope, MetadataKind kind) {
    this(entity, scope, kind, null);
  }

  /**
   * Read a selected set of tags and properties for an entity.
   */
  public Read(MetadataEntity entity, Set<ScopedNameOfKind> selection) {
    this(entity, null, null, selection);
  }

  private Read(MetadataEntity entity,
               @Nullable MetadataScope scope,
               @Nullable MetadataKind kind,
               Set<ScopedNameOfKind> selection) {
    this.entity = entity;
    this.scopes = scope == null ? MetadataScope.ALL : Collections.singleton(scope);
    this.kinds = kind == null ? MetadataKind.ALL : Collections.singleton(kind);
    this.selection = selection;
  }

  /**
   * @return the entity to read the metadata for
   */
  public MetadataEntity getEntity() {
    return entity;
  }

  /**
   * @return the scopes to read from
   */
  public Set<MetadataScope> getScopes() {
    return scopes;
  }

  /**
   * @return the kinds of metadata to read
   */
  public Set<MetadataKind> getKinds() {
    return kinds;
  }

  /**
   * @return the selected tags and properties to read; if non-null,
   *         overrides {@link #getScopes()} and {@link #getKinds()}.
   */
  @Nullable
  public Set<ScopedNameOfKind> getSelection() {
    return selection;
  }

  @Override
  public String toString() {
    return "Read{" +
      "entity=" + entity +
      ", scopes=" + scopes +
      ", kinds=" + kinds +
      ", selection=" + selection +
      '}';
  }
}
