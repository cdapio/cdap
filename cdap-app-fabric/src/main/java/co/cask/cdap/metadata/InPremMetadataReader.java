/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataReaderContext;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.metadata.MetadataRecordV2;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import com.google.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Implementation for {@link MetadataReaderContext} which used {@link MetadataAdmin} to read metadata.
 * This implementation should only be used while running in-prem mode where the {@link MetadataStore} is accessible to
 * the program container.
 * Note: This implementation should not be used in cloud mode.
 */
public class InPremMetadataReader implements MetadataReaderContext {

  private final MetadataAdmin metadataAdmin;

  @Inject
  public InPremMetadataReader(MetadataAdmin metadataAdmin) {
    this.metadataAdmin = metadataAdmin;
  }

  @Override
  public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity) {
    Map<MetadataScope, Metadata> scopeMetadata = new HashMap<>();
    Set<MetadataRecordV2> metadata = metadataAdmin.getMetadata(metadataEntity);
    metadata.forEach(record -> scopeMetadata.put(record.getScope(),
                                                 new Metadata(record.getProperties(), record.getTags())));
    return scopeMetadata;
  }

  @Override
  public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity) {
    final Metadata[] metadata = new Metadata[1];
    metadataAdmin.getMetadata(scope, metadataEntity).forEach(record -> {
      if (record.getScope() == scope) {
        metadata[0] = new Metadata(record.getProperties(), record.getTags());
      }
    });
    return metadata[0];
  }
}
