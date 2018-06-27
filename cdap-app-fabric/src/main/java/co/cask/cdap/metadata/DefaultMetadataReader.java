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
import co.cask.cdap.api.metadata.MetadataReader;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.metadata.MetadataRecordV2;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import com.google.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * <p>{@link MetadataReader} which should be used in local/in-memory mode where {@link MetadataAdmin} can be accessed
 * directly i.e. the process is running as cdap system user and it can access the {@link MetadataDataset} which belongs
 * to cdap user.</p>
 *
 * <p>This implementation should not be used in distributed program container or any process which is not running as
 * cdap system user because the dataset operation will fail due to lack of privileges.</p>
 */
public class DefaultMetadataReader implements MetadataReader {
  private final MetadataAdmin metadataAdmin;

  @Inject
  public DefaultMetadataReader(MetadataAdmin metadataAdmin) {
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
