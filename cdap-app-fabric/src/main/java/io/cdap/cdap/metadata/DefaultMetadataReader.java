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
import io.cdap.cdap.data2.metadata.MetadataCompatibility;
import io.cdap.cdap.spi.metadata.MetadataStorage;

import java.io.IOException;
import java.util.Map;

/**
 * <p>{@link MetadataReader} which should be used in local/in-memory mode where {@link MetadataAdmin} can be accessed
 * directly i.e. the process is running as cdap system user and it can access the {@link MetadataStorage} which belongs
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
  public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity) throws MetadataException {
    try {
      return MetadataCompatibility.toV5Metadata(metadataAdmin.getMetadata(metadataEntity));
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  @Override
  public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity) throws MetadataException {
    try {
      return MetadataCompatibility.toV5Metadata(metadataAdmin.getMetadata(metadataEntity, scope), scope);
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }
}
