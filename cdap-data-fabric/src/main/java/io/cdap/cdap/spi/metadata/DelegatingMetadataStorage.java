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

package io.cdap.cdap.spi.metadata;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import java.io.IOException;
import java.util.List;

/**
 * Delegates {@link MetadataStorage} based on configured extension.
 */
public class DelegatingMetadataStorage implements MetadataStorage {
  private final CConfiguration cConf;
  private final MetadataStorage delegate;
  private static String prefix;
  private static final String SPANNER_METADATA_STORAGE = "gcp-spanner";

  @Inject
  DelegatingMetadataStorage(CConfiguration cConf, MetadataStorageExtensionLoader extensionLoader) throws Exception {
    this.cConf = cConf;
    this.delegate = extensionLoader.get(getName());

    if (this.delegate == null) {
      throw new IllegalArgumentException("Unsupported MetadataProvider type: " + getName());
    }
    // TODO(CDAP-21174): Generalize the context for metadata storage
    if (getName().equals(SPANNER_METADATA_STORAGE)) {
      this.prefix = String.format("%s%s.", Constants.Dataset.STORAGE_EXTENSION_PROPERTY_PREFIX,
                                  SPANNER_METADATA_STORAGE);
    } else {
      this.prefix = null;
    }

    this.delegate.initialize(new DefaultMetadataStorageContext(cConf, prefix));
  }

  @Override
  public void createIndex() throws IOException {
    delegate.createIndex();
  }

  @Override
  public void close() {
    if (delegate != null) {
      delegate.close();
    }
  }

  @Override
  public String getName() {
    return cConf.get(Constants.Metadata.STORAGE_PROVIDER_IMPLEMENTATION);
  }

  @Override
  public void dropIndex() throws IOException {
    delegate.dropIndex();
  }

  @Override
  public MetadataChange apply(MetadataMutation mutation, MutationOptions options) throws IOException {
    return delegate.apply(mutation, options);
  }

  @Override
  public List<MetadataChange> batch(List<? extends MetadataMutation> mutations, MutationOptions options)
    throws IOException {
    return delegate.batch(mutations, options);
  }

  @Override
  public Metadata read(Read read) throws IOException {
    return delegate.read(read);
  }

  @Override
  public SearchResponse search(SearchRequest request) throws IOException {
    return delegate.search(request);
  }
}

