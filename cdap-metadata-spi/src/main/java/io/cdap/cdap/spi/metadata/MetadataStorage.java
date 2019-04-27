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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * The Storage Provider API for Metadata.
 */
@Beta
public interface MetadataStorage extends Closeable {

  /**
   * Create all tables or indexes required for operations.
   */
  void createIndex() throws IOException;

  /**
   * Drop all tables or indexes required for operations.
   */
  void dropIndex() throws IOException;

  /**
   * Apply the given mutation to the metadata state.
   *
   * @param mutation the mutation to perform
   * @return the change effected by this mutation
   */
  MetadataChange apply(MetadataMutation mutation, MutationOptions options) throws IOException;

  /**
   * Apply a batch of mutations to the metadata state.
   *
   * @param mutations the mutations to perform. They are applied in the order given by the list.
   * @return the changes effected by each of the mutations, in the same order as the batch of mutations.
   */
  List<MetadataChange> batch(List<? extends MetadataMutation> mutations, MutationOptions options) throws IOException;

  /**
   * Retrieve the metadata for an entity.
   *
   * @param read the read operation to perform
   * @return the metadata for the entity, never null.
   */
  Metadata read(Read read) throws IOException;

  /**
   * Search the metadata and return matching entities.
   *
   * @param request the search request
   * @return the result of the search, never null.
   */
  SearchResponse search(SearchRequest request) throws IOException;

  /**
   * Close the storage provider. Do not throw exceptions - this will be called
   * when the Metadata service shuts down.
   */
  @Override
  void close();
}
