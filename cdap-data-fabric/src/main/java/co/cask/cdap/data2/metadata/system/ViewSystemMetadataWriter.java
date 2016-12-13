/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.format.RecordFormats;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.id.StreamViewId;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link AbstractSystemMetadataWriter} for an {@link StreamViewId view}.
 */
public class ViewSystemMetadataWriter extends AbstractSystemMetadataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ViewSystemMetadataWriter.class);
  private final StreamViewId viewId;
  private final ViewSpecification viewSpec;
  private final boolean existing;

  public ViewSystemMetadataWriter(MetadataStore metadataStore, StreamViewId viewId, ViewSpecification viewSpec,
                                  boolean existing) {
    super(metadataStore, viewId);
    this.viewId = viewId;
    this.viewSpec = viewSpec;
    this.existing = existing;
  }

  @Override
  protected Map<String, String> getSystemPropertiesToAdd() {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    properties.put(ENTITY_NAME_KEY, viewId.getEntityName());
    if (!existing) {
      properties.put(CREATION_TIME_KEY, String.valueOf(System.currentTimeMillis()));
    }
    return properties.build();
  }

  @Override
  protected String[] getSystemTagsToAdd() {
    return new String[] {
      viewId.getStream()
    };
  }

  @Nullable
  @Override
  protected String getSchemaToAdd() {
    Schema schema = viewSpec.getFormat().getSchema();
    if (schema == null) {
      FormatSpecification format = viewSpec.getFormat();
      RecordFormat<Object, Object> initializedFormat;
      try {
        initializedFormat = RecordFormats.createInitializedFormat(format);
        schema = initializedFormat.getSchema();
      } catch (IllegalAccessException | InstantiationException | UnsupportedTypeException | ClassNotFoundException e) {
        LOG.debug("Exception: ", e);
        LOG.warn("Exception while determining schema for view {}. View {} will not contain schema as metadata.", viewId,
                 viewId);
      }
    }
    return schema == null ? null : schema.toString();
  }
}
