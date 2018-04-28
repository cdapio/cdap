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
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.metadata.MetadataRecord;
import co.cask.cdap.proto.id.EntityId;
import com.google.common.base.Throwables;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * comment
 */
public class DelegatingMetadataReaderContext implements MetadataReaderContext {

  private final MetadataAdmin metadataAdmin;

  public DelegatingMetadataReaderContext(MetadataAdmin metadataAdmin) {
    this.metadataAdmin = metadataAdmin;
  }

  @Override
  public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity) {
    Map<MetadataScope, Metadata> result = new HashMap<>();
    try {
      Set<MetadataRecord> metadata = metadataAdmin.getMetadata(EntityId.fromMetadataEntity(metadataEntity));
      for (MetadataRecord record : metadata) {
        // todo the underlying call should return map
        result.put(record.getScope(), new Metadata(record.getProperties(), record.getTags()));
      }
      return Collections.unmodifiableMap(result);
    } catch (NotFoundException e) {
      throw Throwables.propagate(e);
      // TODO: With the underlying custom MetaadataEntty changes in CDAP-13088 not found will not be thrown
      // so remove this catch block
    }
  }

  @Override
  public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity) {
    Metadata metadata = null;
    try {
      for (MetadataRecord metadataRecord : metadataAdmin.getMetadata(scope,
                                                                     EntityId.fromMetadataEntity(metadataEntity))) {
        // TODO the underlying admin call should return metadata
        metadata = new Metadata(metadataRecord.getProperties(), metadataRecord.getTags());
      }
      return metadata;
    } catch (NotFoundException e) {
      throw Throwables.propagate(e);
      // TODO: With the underlying custom MetaadataEntty changes in CDAP-13088 not found will not be thrown
      // so remove this catch block
    }
  }
}
