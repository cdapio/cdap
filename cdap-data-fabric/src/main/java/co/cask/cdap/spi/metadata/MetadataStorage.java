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

package co.cask.cdap.spi.metadata;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The Storage Provider API for Metadata.
 */
public interface MetadataStorage {

  /**
   * Apply the given mutation to the metadata state.
   *
   * @param mutation the mutation to perform
   * @return the change effected by this mutation
   */
  MetadataChange apply(MetadataMutation mutation) throws IOException;

  /**
   * Apply a batch of mutations to the metadata state.
   *
   * @param mutations the mutations to perform
   * @return the changes effected by each of the mutations
   */
  Collection<MetadataChange> batch(Collection<MetadataMutation> mutations) throws IOException;

  /**
   * Retrieve the metadata for an entity.
   *
   * @param entity the entity
   * @param scope the scope for which to retrieve metadata, or null to indicate all scopes
   * @param kind the kind of metadata to retrieve, or null to retrieve all kinds
   * @param selection a set of scoped tags and properties to retrieve, or null to retrieve all
   *
   * @return the metadata for the entity, never null.
   */
  Metadata read(MetadataEntity entity,
                @Nullable MetadataScope scope,
                @Nullable MetadataKind kind,
                @Nullable Set<ScopedNameOfKind> selection) throws IOException;

  // TODO (CDAP-14584): add search to the interface
}
