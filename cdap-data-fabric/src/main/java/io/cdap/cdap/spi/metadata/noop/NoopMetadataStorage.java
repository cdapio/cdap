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

package io.cdap.cdap.spi.metadata.noop;

import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataChange;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.SearchResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Metadata storage provider that does nothing.
 */
public class NoopMetadataStorage implements MetadataStorage {

  @Override
  public void createIndex() throws IOException {
    // no-op
  }

  @Override
  public void dropIndex() throws IOException {
    // no-op
  }

  @Override
  public MetadataChange apply(MetadataMutation mutation, MutationOptions options) {
    return new MetadataChange(mutation.getEntity(), Metadata.EMPTY, Metadata.EMPTY);
  }

  @Override
  public List<MetadataChange> batch(List<? extends MetadataMutation> mutations, MutationOptions options) {
    return mutations.stream().map(mutation -> apply(mutation, options)).collect(Collectors.toList());
  }

  @Override
  public Metadata read(Read read) {
    return Metadata.EMPTY;
  }

  @Override
  public SearchResponse search(SearchRequest request) {
    return new SearchResponse(request, null, 0, 0, 0, Collections.emptyList());
  }

  @Override
  public void close() {
    // no-op
  }
}
