/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.inject.Inject;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataException;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.metadata.AbstractMetadataClient;
import io.cdap.cdap.common.metadata.MetadataRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Implementation for {@link MetadataReader} which used {@link RemoteMetadataClient} to read metadata.
 * This implementation should only be used while running in-prem mode where the {@link MetadataService} is
 * discoverable.
 * Note: This implementation should not be used in cloud/local mode.
 */
public class RemoteMetadataReader implements MetadataReader {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteMetadataReader.class);

  private final AbstractMetadataClient metadataClient;

  @Inject
  RemoteMetadataReader(AbstractMetadataClient metadataClient) {
    this.metadataClient = metadataClient;
  }

  @Override
  public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity) throws MetadataException {
    Map<MetadataScope, Metadata> scopeMetadata = new HashMap<>();
    Set<MetadataRecord> metadata;
    try {
      metadata = metadataClient.getMetadata(metadataEntity);
    } catch (ServiceUnavailableException e) {
      throw e;
    } catch (Exception e) {
      throw new MetadataException(e);
    }
    metadata.forEach(record -> scopeMetadata.put(record.getScope(),
                                                 new Metadata(record.getProperties(), record.getTags())));
    LOG.trace("Returning metadata record {} for {}", scopeMetadata, metadataEntity);
    return scopeMetadata;
  }

  @Override
  public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity) throws MetadataException {
    Metadata metadata = getMetadata(metadataEntity).get(scope);
    LOG.trace("Returning metadata {} for {} in scope {}", metadata, metadataEntity, scope);
    return metadata;
  }
}
