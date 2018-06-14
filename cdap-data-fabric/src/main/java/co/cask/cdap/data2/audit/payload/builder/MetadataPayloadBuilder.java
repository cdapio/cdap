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

package co.cask.cdap.data2.audit.payload.builder;

import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.metadata.MetadataRecordV2;
import co.cask.cdap.proto.audit.payload.metadata.MetadataPayload;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder for {@link MetadataPayload}.
 */
public class MetadataPayloadBuilder {
  private final Map<MetadataScope, Metadata> previous = new HashMap<>();
  private final Map<MetadataScope, Metadata> additions = new HashMap<>();
  private final Map<MetadataScope, Metadata> deletions = new HashMap<>();

  /**
   * Add the previous value of metadata.
   *
   * @param record previous value of metadata record
   * @return the builder object
   */
  public MetadataPayloadBuilder addPrevious(MetadataRecordV2 record) {
    previous.put(record.getScope(), new Metadata(record.getProperties(), record.getTags()));
    return this;
  }

  /**
   * Add the additions to the previous value of metadata to get the current value of metadata.
   *
   * @param record additions to the metadata
   * @return the builder object
   */
  public MetadataPayloadBuilder addAdditions(MetadataRecordV2 record) {
    additions.put(record.getScope(), new Metadata(record.getProperties(), record.getTags()));
    return this;
  }

  /**
   * Add the deletions to the previous value of metadata to get the current value of metadata.
   *
   * @param record deletions to metadata
   * @return the builder object
   */
  public MetadataPayloadBuilder addDeletions(MetadataRecordV2 record) {
    deletions.put(record.getScope(), new Metadata(record.getProperties(), record.getTags()));
    return this;
  }

  /**
   * Build the metadata payload using the previous value, additions and deletions.
   *
   * @return the metadata payload built
   */
  public MetadataPayload build() {
    return new MetadataPayload(previous, additions, deletions);
  }
}
