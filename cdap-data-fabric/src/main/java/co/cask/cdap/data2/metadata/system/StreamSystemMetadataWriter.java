/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.system;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link AbstractSystemMetadataWriter} for a {@link Id.Stream stream}.
 */
public class StreamSystemMetadataWriter extends AbstractSystemMetadataWriter {

  private final StreamConfig config;

  public StreamSystemMetadataWriter(MetadataStore metadataStore, Id.Stream streamId, StreamConfig config) {
    super(metadataStore, streamId);
    this.config = config;
  }

  @Override
  Map<String, String> getSystemPropertiesToAdd() {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    properties.put(TTL_KEY, String.valueOf(config.getTTL()));
    return properties.build();
  }

  @Override
  String[] getSystemTagsToAdd() {
    return new String[] {
      config.getStreamId().getId()
    };
  }

  @Nullable
  @Override
  String getSchemaToAdd() {
    Schema schema = config.getFormat().getSchema();
    return schema == null ? null : schema.toString();
  }
}
