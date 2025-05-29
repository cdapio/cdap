/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.metadata.spanner;

import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataChange;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MetadataStorageContext;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.Read;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.SearchResponse;
import java.io.IOException;
import java.util.List;

/**
 * A metadata storage provider that delegates to Spanner.
 */
public class SpannerMetadataStorage implements MetadataStorage {

  @Override
  public void initialize(MetadataStorageContext context) throws Exception {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public void close() {
  }

  @Override
  public void createIndex() throws IOException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public String getName() {
    return "gcp-spanner";
  }

  @Override
  public void dropIndex() throws IOException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public MetadataChange apply(MetadataMutation mutation, MutationOptions options) throws IOException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public List<MetadataChange> batch(List<? extends MetadataMutation> mutations, MutationOptions options)
    throws IOException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public Metadata read(Read read) throws IOException {
    throw new IOException("NOT IMPLEMENTED");
  }

  @Override
  public SearchResponse search(SearchRequest request) throws IOException {
    throw new IOException("NOT IMPLEMENTED");
  }
}

