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

package co.cask.cdap.messaging;

import co.cask.cdap.data2.transaction.coprocessor.CacheSupplier;
import co.cask.cdap.data2.transaction.queue.hbase.coprocessor.CConfigurationReader;
import co.cask.cdap.data2.util.ReferenceCountedSupplier;
import co.cask.cdap.data2.util.hbase.ScanBuilder;
import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

/**
 * Supplies instances of {@link TopicMetadataCache} implementations.
 */
public class TopicMetadataCacheSupplier implements CacheSupplier<TopicMetadataCache> {
  private static final ReferenceCountedSupplier<TopicMetadataCache> referenceCountedSupplier =
    new ReferenceCountedSupplier<>(TopicMetadataCache.class.getSimpleName());

  private final Supplier<TopicMetadataCache> supplier;

  public TopicMetadataCacheSupplier(final RegionCoprocessorEnvironment env, final CConfigurationReader cConfReader,
                                    final String hbaseNamespacePrefix, final String metadataTableNamespace,
                                    final ScanBuilder scanBuilder) {
    this.supplier = new Supplier<TopicMetadataCache>() {
      @Override
      public TopicMetadataCache get() {
        return new TopicMetadataCache(env, cConfReader, hbaseNamespacePrefix, metadataTableNamespace, scanBuilder);
      }
    };
  }

  @Override
  public TopicMetadataCache get() {
    return referenceCountedSupplier.getOrCreate(supplier);
  }

  @Override
  public void release() {
    referenceCountedSupplier.release();
  }
}
