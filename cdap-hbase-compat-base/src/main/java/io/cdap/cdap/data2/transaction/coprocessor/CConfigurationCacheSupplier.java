/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.data2.transaction.coprocessor;

import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.tephra.coprocessor.ReferenceCountedSupplier;

/**
 * Supplies instances of {@link CConfigurationCache} implementations.
 */
public class CConfigurationCacheSupplier implements CacheSupplier<CConfigurationCache> {

  private static final ReferenceCountedSupplier<CConfigurationCache> referenceCountedSupplier
      = new ReferenceCountedSupplier<>(CConfigurationCache.class.getSimpleName());

  private final Supplier<CConfigurationCache> supplier;

  public CConfigurationCacheSupplier(final CoprocessorEnvironment env, final String tablePrefix,
      final String maxLifetimeProperty, final int defaultMaxLifetime) {
    this.supplier = new Supplier<CConfigurationCache>() {
      @Override
      public CConfigurationCache get() {
        return new CConfigurationCache(env, tablePrefix, maxLifetimeProperty, defaultMaxLifetime);
      }
    };
  }

  @Override
  public CConfigurationCache get() {
    return referenceCountedSupplier.getOrCreate(supplier);
  }

  @Override
  public void release() {
    referenceCountedSupplier.release();
  }
}
