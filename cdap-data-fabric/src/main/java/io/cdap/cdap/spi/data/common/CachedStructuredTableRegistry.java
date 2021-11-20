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

package io.cdap.cdap.spi.data.common;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Wraps a StructuredTableRegistry with a cache that caches specification of an existing table.
 * This is to prevent multiple specification lookups from the underlying table during transactions.
 * The specification of a table is not expected to change frequently.
 */
public class CachedStructuredTableRegistry implements StructuredTableRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(CachedStructuredTableRegistry.class);
  private static final int MAX_CACHE_SIZE = 100;

  private final StructuredTableRegistry delegate;
  private final LoadingCache<StructuredTableId, Optional<StructuredTableSpecification>> specCache;

  public CachedStructuredTableRegistry(StructuredTableRegistry delegate) {
    LOG.debug("Enabling cache for structured table registry lookups.");
    this.delegate = delegate;
    this.specCache = CacheBuilder.newBuilder()
      .maximumSize(MAX_CACHE_SIZE)
      .build(new CacheLoader<StructuredTableId, Optional<StructuredTableSpecification>>() {
        @Override
        public Optional<StructuredTableSpecification> load(StructuredTableId tableId) {
          return getSpecificationFromDelegate(tableId);
        }
      });
  }

  @Override
  public void registerSpecification(StructuredTableSpecification specification)
    throws IOException, TableAlreadyExistsException {
    delegate.registerSpecification(specification);
    specCache.invalidate(specification.getTableId());
  }

  @Override
  @Nullable
  public StructuredTableSpecification getSpecification(StructuredTableId tableId) {
    Optional<StructuredTableSpecification> optional = specCache.getUnchecked(tableId);
    if (optional.isPresent()) {
      return optional.get();
    }
    // If spec is not available in the cache, then invalidate the cache for tableId to force cache reload,
    // and read again to see if the table is now created
    specCache.invalidate(tableId);
    optional = specCache.getUnchecked(tableId);
    return optional.orElse(null);
  }

  @Override
  public void removeSpecification(StructuredTableId tableId) {
    delegate.removeSpecification(tableId);
    specCache.invalidate(tableId);
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  private Optional<StructuredTableSpecification> getSpecificationFromDelegate(StructuredTableId tableId) {
    StructuredTableSpecification spec = delegate.getSpecification(tableId);
    return spec == null ? Optional.empty() : Optional.of(spec);
  }
}
